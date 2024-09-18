function applyDateMask(input) {
    input.addEventListener('input', function(event) {
        let value = input.value.replace(/[^0-9]/gm, '');
        value = value.substring(0, 8);
        
        const segments = [
            value.substring(0, 2), 
            value.substring(2, 4),
            value.substring(4, 8)
        ];

        input.value = [
            segments[0],
            segments[1].length >= 1 ? '/' : '',
            segments[1],
            segments[2].length >= 1 ? '/' : '',
            segments[2]
        ].join('');
    });
}

function cleanErrorOnInput(input) {
    input.addEventListener('input', function(event) {
        hideErrorMessage(input)
    })
}

const inputs = document.querySelectorAll('input');
const initialDateInput = document.getElementById('initial_date');
const finalDateInput = document.getElementById('final_date');
const emailInput = document.getElementById('email');
const submitButton = document.querySelector('.submit');

applyDateMask(initialDateInput);
applyDateMask(finalDateInput);
cleanErrorOnInput(initialDateInput);
cleanErrorOnInput(finalDateInput);
cleanErrorOnInput(emailInput);

document.getElementById('dataForm').addEventListener('submit', async function(event) {
    event.preventDefault();

    const initialDate = initialDateInput.value;
    const finalDate = finalDateInput.value;
    const email = emailInput.value;

    resetMessages();
    hideAlert();

    let hasError = false;

    if(!initialDate){
        showErrorMessage('initial_date_error', "Campo obrigatório");
        hasError = true;
    }
    else if (!validateDate(initialDate)) {
        showErrorMessage('initial_date_error', "Campo inválido");
        hasError = true;
    }

    if(!finalDate){
        showErrorMessage('final_date_error', "Campo obrigatório");
        hasError = true;
    }
    else if (!validateDate(finalDate)) {
        showErrorMessage('final_date_error', "Campo inválido");
        hasError = true;
    }

    if(!email){
        showErrorMessage('email_error', "Campo obrigatório");
        hasError = true;
    }
    else if (!validateEmail(email)) {
        showErrorMessage('email_error', "Campo inválido");
        hasError = true;
    }

    if (hasError) {
        return;
    }

    const formData = {
        initial_date: convertDate(initialDate),
        final_date: convertDate(finalDate),
        email: email
    };

    try {
        setLoadingState(true);

        const response = await fetch('/api/submit', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });

        const result = await response.json();
        
        if (result.success) {
            showAlert('Relatório requisitado com sucesso.');
        } else {
            showAlert(`Error: ${result.error}`, 'danger');
        }
    } catch (error) {
        showAlert(`Error: ${error.message}`, 'danger');
    } finally {
        setLoadingState(false);
    }
});

function validateDate(date) {
    const re = /^(0[1-9]|[12][0-9]|3[01])\/(0[1-9]|1[0-2])\/(19|20)\d{2}$/;
    return re.test(date);
}

function validateEmail(email) {
    const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return re.test(String(email).toLowerCase());
}

function convertDate(date) {
    const [day, month, year] = date.split('/')
    return `${year}-${month}-${day}`
}

function showErrorMessage(elementId, message) {
    const errorElement = document.getElementById(elementId);
    if (errorElement) {
        errorElement.textContent = message;
        errorElement.style.display = 'block';
    }
}

function hideErrorMessage(element) {
    const errorElement = document.getElementById(`${element.id}_error`);
    if (errorElement) {
        errorElement.textContent = "";
        errorElement.style.display = 'none';
    }
}

function showAlert(message, type = "success") {
    const alertElement = document.getElementById('alert');
    alertElement.textContent = message;
    alertElement.style.display = 'block';
    alertElement.classList.remove(...['success', 'danger']);
    alertElement.classList.add(type);
}

function hideAlert() {
    const alertElement = document.getElementById('alert');
    alertElement.textContent = "";
    alertElement.style.display = 'none';
}

function resetMessages() {
    const errorMessages = document.querySelectorAll('.error-message');
    errorMessages.forEach(function(element) {
        element.style.display = 'none';
    });
}

function setLoadingState(isLoading) {
    if (isLoading) {
        submitButton.disabled = true;
        submitButton.classList.add('loading');
        submitButton.textContent = 'Enviando...';

        inputs.forEach(input => {
            input.disabled = true;
        });

    } else {
        submitButton.disabled = false;
        submitButton.classList.remove('loading');
        submitButton.textContent = 'Enviar';

        inputs.forEach(input => {
            input.disabled = false;
        });

    }
}