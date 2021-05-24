import uuid

from flask import Flask, jsonify, request

from service.emp_mgmt import EmployeeMgmt

app = Flask(__name__)


@app.route('/create-db', methods=['POST'])
def create_db():
    """
    This method creates database.
    :return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            emp_mgmt = EmployeeMgmt(transaction_id)
            emp_mgmt.create_database()
        else:
            return jsonify('FAILURE: Please use POST request.')
    except Exception as e:
        return jsonify(f'FAILURE: {e}')
    return jsonify('SUCCESS')


@app.route('/create-table', methods=['POST'])
def create_table():
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            emp_mgmt = EmployeeMgmt(transaction_id)
            emp_mgmt.create_table()
        else:
            return jsonify('FAILURE: Please use POST request.')
    except Exception as e:
        return jsonify(f'FAILURE: {e}')
    return jsonify('SUCCESS')


@app.route('/save-emp', methods=['POST'])
def save_emp():
    """
    This method used to save employee.\n
    return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            employee = request.json['employee']
            emp_mgmt = EmployeeMgmt(transaction_id)
            emp_mgmt.save_employee(employee)
        else:
            return jsonify('FAILURE: Please use POST request.')
    except Exception as e:
        return jsonify(f'FAILURE: {e}')
    return jsonify('In save employee.')


@app.route('/get-emp-by-id', methods=['POST'])
def get_emp_by_id():
    """
    This method used to get employee by id.
    return: Employee
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            emp_id = eval(request.json['emp_id'])
            emp_mgmt = EmployeeMgmt(transaction_id)
            emp_mgmt.get_employee_by_id(emp_id)
        else:
            return jsonify('FAILURE: Please use POST request.')
    except Exception as e:
        return jsonify(f'FAILURE: {e}')
    return jsonify('In get-emp')


@app.route('/get-all-emp', methods=['POST'])
def get_all_emp():
    """
    This method used to get all employees.
    return: Employee list
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            emp_mgmt = EmployeeMgmt(transaction_id)
            emp_mgmt.get_all_employee()
        else:
            return jsonify('FAILURE: Please use POST request.')
    except Exception as e:
        return jsonify(f'FAILURE: {e}')
    return jsonify('In get-all-emp.')


if __name__ == '__main__':
    app.run()
