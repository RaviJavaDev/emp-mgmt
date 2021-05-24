from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/save-emp', methods=['POST'])
def save_emp():
    """
    This method used to save employee.\n
    return: SUCCESS/FAILURE
    """
    return jsonify('In save employee.')


@app.route('/get-emp-by-id', methods=['GET'])
def get_emp_by_id():
    """
    This method used to get employee by id.
    return: Employee
    """
    return jsonify('In get-emp')


@app.route('/get-all-emp', methods=['GET'])
def get_all_emp():
    """
    This method used to get all employees.
    return: Employee list
    """
    return jsonify('In get-all-emp.')


if __name__ == '__main__':

    app.run()
