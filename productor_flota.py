#!/usr/bin/env python3
"""
productor_flota.py - CASO A: Monitoreo de flota de vehículos.
Emite eventos de telemetría al topic 'flota'. Siembra patrones a detectar:
  - exceso de velocidad SOSTENIDO
  - caída anómala de combustible (posible robo/fuga)
  - vehículos que dejan de emitir (silencio = posible falla)

Requiere:  pip install kafka-python
Uso:       python3 productor_flota.py
Detener:   Ctrl+C
"""
import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

VEHICULOS = [f"V{n:02d}" for n in range(1, 9)]   
combustible = {v: 100.0 for v in VEHICULOS}
mudo_desde = random.randint(60, 90)

TOPIC = "flota"
print(f"Enviando telemetría de flota al topic '{TOPIC}'. Ctrl+C para parar.")
print("Patrones sembrados: exceso sostenido, robo de combustible, y un vehículo que enmudece.")

i = 0
try:
    while True:
        i += 1
        for v in VEHICULOS:
            if v == "V07" and i >= mudo_desde:
                continue

            if v == "V03" and 20 <= (i % 60) <= 35:
                velocidad = random.randint(115, 140)
            else:
                velocidad = random.randint(30, 95)

            consumo = random.uniform(0.05, 0.2)
            if v == "V05" and 40 <= (i % 80) <= 55:
                consumo = random.uniform(2.5, 4.0)      
            combustible[v] = max(0.0, combustible[v] - consumo)
            if combustible[v] < 5:
                combustible[v] = 100.0                

            evento = {
                "id_vehiculo": v,
                "velocidad": velocidad,
                "combustible": round(combustible[v], 1),
                "lat": round(6.24 + random.uniform(-0.1, 0.1), 5),
                "lon": round(-75.58 + random.uniform(-0.1, 0.1), 5),
            }
            producer.send(TOPIC, evento)
        print(f"  tick {i}: emitidos {len(VEHICULOS)} vehículos (V07 calla desde tick {mudo_desde})")
        time.sleep(1.0)
except KeyboardInterrupt:
    print("\nDeteniendo..."); producer.flush(); producer.close(); print("Productor cerrado.")
