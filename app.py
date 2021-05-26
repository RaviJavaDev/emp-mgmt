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
            db_name = request.json['db_name']
            db = request.json['db']
            emp_mgmt = EmployeeMgmt(transaction_id, db)
            emp_mgmt.create_database(db_name)
        return jsonify({'Status': 'SUCCESS', 'message': f'Database {db_name} created successfully.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/create-table', methods=['POST'])
def create_table():
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            columns = request.json['columns']
            db = request.json['db']
            emp_mgmt = EmployeeMgmt(transaction_id, db)
            emp_mgmt.create_table(db_name, table_name, columns)
        return jsonify({'Status': 'SUCCESS', 'message': f'Table {table_name} created successfully.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/save-record', methods=['POST'])
def save_emp():
    """
    This method used to save employee.\n
    return: SUCCESS/FAILURE
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            db = request.json['db']
            dct_obj = request.json['dct_obj']
            emp_mgmt = EmployeeMgmt(transaction_id, db)
            emp_mgmt.save_employee(db_name, table_name, dct_obj)
        return jsonify({'Status': 'SUCCESS', 'message': 'Record saved successfully.'})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/get-record-by-id', methods=['POST'])
def get_emp_by_id():
    """
    This method used to get employee by id.
    return: Employee
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            db = request.json['db']
            _id = request.json['_id']
            emp_mgmt = EmployeeMgmt(transaction_id, db)
            records = emp_mgmt.get_employee_by_id(db_name, table_name, _id)
        return jsonify({'Status': 'SUCCESS', 'data': records})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


@app.route('/get-all-records', methods=['POST'])
def get_all_emp():
    """
    This method used to get all employees.
    return: Employee list
    """
    try:
        if request.method == 'POST':
            transaction_id = str(uuid.uuid4())
            db_name = request.json['db_name']
            table_name = request.json['table_name']
            db = request.json['db']
            emp_mgmt = EmployeeMgmt(transaction_id, db)
            records = emp_mgmt.get_all_employee(db_name, table_name)
        return jsonify({'Status': 'SUCCESS', 'data': records})
    except Exception as e:
        return jsonify({'Status': 'FAILURE', 'message': f'FAILURE: {e}'})


if __name__ == '__main__':
    app.run()
