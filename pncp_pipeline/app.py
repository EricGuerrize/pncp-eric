import subprocess
from flask import Flask, jsonify
from flask_cors import CORS
import os
import sys

app = Flask(__name__)
CORS(app) # Allow all origins so frontend can call it directly

@app.route('/sync/<municipio>', methods=['GET', 'POST'])
def sync_pipeline(municipio):
    # Municipio slug example: rondolandia, acorizal, jangada, lucas_do_rio_verde
    
    pipeline_script = os.path.join(os.path.dirname(__file__), 'pipeline_multicidades.py')
    
    # We call the generic pipeline multicidades
    # E.g: python pipeline_multicidades.py --cidades rondolandia --ano 2026
    command = [sys.executable, pipeline_script, '--cidades', municipio, '--ano', '2026']

    print(f"Executando sync via API para: {municipio}")
    
    try:
        # Run subprocess and wait for completion
        result = subprocess.run(command, capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        if result.returncode != 0:
            print("PIPELINE ERROR:")
            print(result.stderr)
            return jsonify({'status': 'error', 'message': f"Pipeline crash:\n{result.stderr}"}), 500
        
        return jsonify({
            'status': 'success', 
            'message': f"Pipeline para {municipio} rodou e enviou para o Firebase com sucesso!",
            'logs': result.stdout
        })

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    print("Iniciando Servidor Flask na porta 5000...")
    print("Endpoint disponível: GET http://localhost:5000/sync/<municipio>")
    app.run(port=5000, debug=True)
