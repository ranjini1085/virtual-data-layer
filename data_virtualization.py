def getopts(argv):
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found a "-name value" pair.
            opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts


if __name__ == '__main__':
    from sys import argv
    myargs = getopts(argv)
    if '-i' in myargs:  # Example usage.
        print("[['c_mktsegment', 'sum_c_acctbal', 'avg_c_acctbal', 'min_c_acctbal', 'max_c_acctbal', 'count'], ['BUILDING', '15074.08', '7537.04', '7140.55', '7933.53', '2']]")
