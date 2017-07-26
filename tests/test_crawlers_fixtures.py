import pytest
import settings

def executeScriptsFromFile(filename, c):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            c.execute(command)
        except Exception as msg:
            print("Command skipped: ", msg)

@pytest.fixture
def test_db():
    return settings.DB

def clean_test_db():
    db = settings.DB
    c = db.cursor()
    executeScriptsFromFile("./sql/db_init_mysql.sql", c)
    executeScriptsFromFile("./sql/db_load_persons.sql", c)
    executeScriptsFromFile("./sql/db_load_keywords.sql", c)
    executeScriptsFromFile("./sql/db_load_sites.sql", c)
    c.close()
    return db