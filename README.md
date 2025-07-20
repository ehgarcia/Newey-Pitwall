# Newey-Pitwall
Adrian Newey es sin duda el ingeniero más galardonado de la era moderna de la F1. ha diseñado coches que han ganado 13 títulos de pilotos y 12 títulos de constructores con tres equipos diferentes (Williams, McLaren y Red Bull).


<details> <summary><strong>Entorno local</strong></summary>
git clone git@github.com:tuUsuario/Newey-Pitwall.git
cd Newey-Pitwall
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # y ajusta si cambias contraseñas o puertos

</details> <details> <summary><strong>Lanzar servicios (sin Docker Compose)</strong></summary>
bash scripts/start_rabbitmq.sh
bash scripts/start_postgres.sh
# ejecútalo una vez para crear la tabla
docker exec -i pg-f1 psql -U postgres -f sql/init_db.sql

</details> <details> <summary><strong>Ejecutar apps</strong></summary>
# Terminal 1: listener
python listener/listener.py

# Terminal 2: consumer
python consumer/consumer_db.py
