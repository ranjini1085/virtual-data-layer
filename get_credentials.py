def get_credentials(credential_file_name):
    '''reads in database credentials stored in a text file

    keyword args:
        credential_file_name = string containing credential file name
            assumption is that the credential file name will contain the database url, database name, username, and password,
            and be formatted something like this:

            database_url = foo
            database_name = bar
            username = blah
            password = blo

    returns:
        a dictionary of database credentials read from the file

'''
    credentials = {}
    
    with open(credential_file_name) as credential_file:
        credential_file_contents = credential_file.read().splitlines()
        
        
        
        for i,v in enumerate(credential_file_contents):
            if v.find('database_url=') != -1:
                credentials['database_url'] = v.replace('database_url=','')
            if v.find('database_name=') != -1:
                credentials['database_name'] = v.replace('database_name=','')
            if v.find('username=') != -1:
                credentials['username'] = v.replace('username=','')
            if v.find('password=') != -1:
                credentials['password'] = v.replace('password=','')
            
    return credentials