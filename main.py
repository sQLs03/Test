from flask import Flask
import function

app = Flask(__name__)
id_list = function.create_output_id_list()


@app.route('/crimes/<int:start>/<int:stop>', methods=['GET'])
def get_crimes_info(start, stop):
    if stop < start or stop - start > 20:
        return "Incorrect data"
    output = function.output_info_dict(id_list, start, stop - start)
    return output


if __name__ == "__main__":
    app.run()
