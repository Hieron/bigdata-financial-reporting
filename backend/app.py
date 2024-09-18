from flask import Flask, render_template, request, jsonify
import requests

app = Flask(__name__)

CONTROLLER_URL = 'http://controller:6000'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/jobs', methods=['GET'])
def list_jobs():
    try:
        endpoint_url = f"{CONTROLLER_URL}/api/jobs"

        response = requests.get(endpoint_url)
        response.raise_for_status()  

        jobs = response.json()

        return render_template('jobs.html', jobs=jobs)

    except requests.exceptions.RequestException as e:
        return render_template('jobs.html', jobs=[], error=str(e))

@app.route('/api/submit', methods=['POST'])
def submit():
    data = request.json
    
    endpoint_url = f"{CONTROLLER_URL}/api/schedule"
    
    initial_date = data.get('initial_date')
    final_date = data.get('final_date')
    email = data.get('email')

    if not initial_date or not final_date or not email:
        return jsonify({'success': False, 'error': 'Todos os campos são obrigatórios!'}), 400

    controller_payload = {
        'script_path': '/tmp/data/script.py',
        'initial_date': initial_date,
        'final_date': final_date,
        'email': email
    }

    try:
        response = requests.post(endpoint_url, json=controller_payload, timeout=5*60)
        
        if response.status_code == 200:
            return jsonify({
                'success': True,
                'message': 'Job enviado ao Controller com sucesso!',
                # 'controller_response': response.json()
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Erro ao chamar o Controller: {response.text}'
            }), response.status_code

    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': 'Não foi possível conectar ao serviço do Controller. Verifique se o serviço está em execução e acessível.'
        }), 500

    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': 'A requisição ao Controller demorou muito para responder. Tente novamente mais tarde.'
        }), 500

    except requests.exceptions.RequestException as e:
        if isinstance(e, requests.exceptions.NameResolutionError):
            return jsonify({
                'success': False,
                'error': 'Erro ao resolver o nome do serviço Controller. Verifique se o nome do host está correto.'
            }), 500
        return jsonify({
            'success': False,
            'error': f'Erro ao se conectar com o Controller: {e}'
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
