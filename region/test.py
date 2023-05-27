
def reform(input_tuple):
    data, headers = input_tuple
    result = ','.join(headers) + '\n'
    for row in data:
        row_str = ','.join(str(value) for value in row)
        result += row_str + '\n'
    result += 'send end'
    return result


your_tuple = ([['3160115278', 'qiu', 20, 177.5], ['3160115279', 'qiu', 20, 177.5]], ['ID', 'name', 'age', 'height'])
result_str = reform(your_tuple)
print(result_str)

