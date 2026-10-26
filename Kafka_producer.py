import time
import json
import random
from kafka import KafkaProducer

def generar_nota_estudiante():
    materias = ["Matematicas", "Programacion", "Bases_de_Datos", "Ciencias"]
    return {
        "estudiante_id": random.randint(100, 110),
        "materia": random.choice(materias),
        "nota": round(random.uniform(1.0, 5.0), 1),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    datos = generar_nota_estudiante()
    producer.send('notas_estudiantes', value=datos)
    print(f"Nota enviada: {datos}")
    time.sleep(1)
