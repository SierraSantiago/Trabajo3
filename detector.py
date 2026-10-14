import csv
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Configuración de las tres reglas.
UMBRAL_VELOCIDAD = 100          # km/h, estrictamente mayor
LECTURAS_MINIMAS = 3            # dentro de 30 segundos
UMBRAL_CAIDA = 2.0              # unidades por lectura
UMBRAL_SILENCIO = 15            # segundos sin telemetría

# Los resultados de una ejecución se guardan en archivos CSV.
EVIDENCIAS = Path(__file__).resolve().parent / "evidencias"
EVIDENCIAS.mkdir(exist_ok=True)
ARCHIVO_ALERTAS = EVIDENCIAS / "alertas.csv"
ARCHIVO_RESUMEN = EVIDENCIAS / "resumen_alertas.csv"

with ARCHIVO_ALERTAS.open("w", encoding="utf-8", newline="") as archivo:
    csv.writer(archivo).writerow(["fecha_alerta", "regla", "vehiculo", "detalle"])

bloqueo_estado = threading.Lock()
bloqueo_registro = threading.Lock()
detener_monitor = threading.Event()
conteos = Counter()

# Estado local de las reglas 2 y 3.
ultimo_combustible = {}
ultima_emision = {}
silencios_reportados = set()

# La regla 1 usa ventanas deslizantes que se superponen. Este diccionario
# fusiona ventanas contiguas/solapadas en un solo incidente de velocidad.
fin_episodio_velocidad = {}


def registrar_alerta(regla, vehiculo, detalle):
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with bloqueo_registro:
        with ARCHIVO_ALERTAS.open("a", encoding="utf-8", newline="") as archivo:
            csv.writer(archivo).writerow([fecha, regla, vehiculo, detalle])
        conteos[(vehiculo, regla)] += 1
    print(f"\n[ALERTA {regla}] Vehículo: {vehiculo} | {detalle}", flush=True)


def registrar_velocidad(batch_df, batch_id):
    """Deduplica actualizaciones de ventana y agrupa ventanas solapadas."""
    ventanas = (
        batch_df.select("id_vehiculo", "inicio", "fin", "lecturas_exceso")
        .orderBy("id_vehiculo", "inicio")
        .collect()
    )
    for ventana in ventanas:
        vehiculo = ventana["id_vehiculo"]
        inicio = ventana["inicio"]
        fin = ventana["fin"]
        ultimo_fin = fin_episodio_velocidad.get(vehiculo)

        # Primera ventana cualificada de un episodio. Las siguientes ventanas
        # del mismo episodio se superponen y solo amplían su fin.
        if ultimo_fin is None or inicio > ultimo_fin:
            registrar_alerta(
                "VELOCIDAD",
                vehiculo,
                f"> {UMBRAL_VELOCIDAD} km/h en "
                f"{ventana['lecturas_exceso']} lecturas; "
                f"ventana {inicio} a {fin}",
            )

        if ultimo_fin is None or fin > ultimo_fin:
            fin_episodio_velocidad[vehiculo] = fin


def procesar_telemetria(batch_df, batch_id):
    """Detecta caídas entre lecturas y actualiza última actividad."""
    # El topic del ejercicio se creó con una sola partición. Ordenar por
    # offset conserva el orden de los eventos dentro de esta partición.
    lecturas = (
        batch_df.select(
            "id_vehiculo", "combustible", "fecha_recepcion",
            "kafka_partition", "kafka_offset"
        )
        .orderBy("kafka_partition", "kafka_offset")
        .collect()
    )
    for lectura in lecturas:
        vehiculo = lectura["id_vehiculo"]
        actual = lectura["combustible"]
        if vehiculo is None:
            continue

        with bloqueo_estado:
            ultima_emision[vehiculo] = time.monotonic()
            recuperado = vehiculo in silencios_reportados
            silencios_reportados.discard(vehiculo)

        if recuperado:
            print(f"\n[RECUPERADO] {vehiculo} volvió a transmitir", flush=True)

        if actual is None:
            continue
        anterior = ultimo_combustible.get(vehiculo)
        if anterior is not None:
            caida = round(anterior - actual, 2)
            if caida >= UMBRAL_CAIDA:
                registrar_alerta(
                    "COMBUSTIBLE",
                    vehiculo,
                    f"anterior={anterior:.1f}; actual={actual:.1f}; "
                    f"caida={caida:.1f}; evento={lectura['fecha_recepcion']}",
                )
        ultimo_combustible[vehiculo] = actual


def vigilar_silencio():
    """Revisa el silencio aunque no lleguen mensajes nuevos del vehículo."""
    while not detener_monitor.wait(2):
        ahora = time.monotonic()
        alertas = []
        with bloqueo_estado:
            for vehiculo, instante in ultima_emision.items():
                segundos = ahora - instante
                if segundos >= UMBRAL_SILENCIO and vehiculo not in silencios_reportados:
                    silencios_reportados.add(vehiculo)
                    alertas.append((vehiculo, segundos))
        for vehiculo, segundos in alertas:
            registrar_alerta(
                "SILENCIO", vehiculo,
                f"sin_transmitir={segundos:.1f}s; umbral={UMBRAL_SILENCIO}s",
            )


def guardar_resumen():
    with bloqueo_registro:
        resumen = dict(conteos)

    with ARCHIVO_RESUMEN.open("w", encoding="utf-8", newline="") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(["vehiculo", "regla", "cantidad_alertas"])
        for (vehiculo, regla), cantidad in sorted(resumen.items()):
            escritor.writerow([vehiculo, regla, cantidad])

    print("\nRESUMEN DE LA EJECUCIÓN")
    if not resumen:
        print("No se registraron alertas.")
    else:
        for (vehiculo, regla), cantidad in sorted(resumen.items()):
            print(f"  {vehiculo} | {regla:<12} | {cantidad} alerta(s)")
    print(f"Detalle: {ARCHIVO_ALERTAS}")
    print(f"Resumen: {ARCHIVO_RESUMEN}")


spark = (
    SparkSession.builder
    .appName("DetectorFlota")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8",
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("id_vehiculo", StringType()),
    StructField("velocidad", IntegerType()),
    StructField("combustible", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
])

kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "flota")
    .option("startingOffsets", "latest")
    .load()
)

eventos = (
    kafka.select(
        col("timestamp").alias("fecha_recepcion"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        from_json(col("value").cast("string"), schema).alias("datos"),
    )
    .select("fecha_recepcion", "kafka_partition", "kafka_offset", "datos.*")
    .filter(col("id_vehiculo").isNotNull())
)

# REGLA 1: conteo de >100 km/h en ventanas de 30 s, cada 10 s.
exceso = (
    eventos.withWatermark("fecha_recepcion", "1 minute")
    .filter(col("velocidad") > UMBRAL_VELOCIDAD)
    .groupBy(window("fecha_recepcion", "30 seconds", "10 seconds"), "id_vehiculo")
    .count()
    .filter(col("count") >= LECTURAS_MINIMAS)
    .select(
        "id_vehiculo", col("window.start").alias("inicio"),
        col("window.end").alias("fin"),
        col("count").alias("lecturas_exceso"),
    )
)

query_velocidad = (
    exceso.writeStream.foreachBatch(registrar_velocidad)
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start()
)

# REGLAS 2 Y 3: lectura consecutiva / última emisión conocida.
query_telemetria = (
    eventos.writeStream.foreachBatch(procesar_telemetria)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

hilo_monitor = threading.Thread(target=vigilar_silencio, daemon=True)
hilo_monitor.start()

print("\nDetector iniciado: VELOCIDAD, COMBUSTIBLE y SILENCIO")
print("Inicia ahora productor_flota.py en otra terminal.")
print("Detén el detector con Ctrl+C ANTES de detener el productor.\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nDeteniendo detector...")
finally:
    detener_monitor.set()
    hilo_monitor.join(timeout=3)
    for query in (query_velocidad, query_telemetria):
        if query.isActive:
            query.stop()
    guardar_resumen()
    spark.stop()
