from neo4j_package.initialise_schema import initialise_schema

neo4j_config = {
    'url': 'neo4j+ssc://bc1e22a5.databases.neo4j.io',
    'database': 'neo4j',
    'user': 'neo4j',
    'password': '-'
}

initialise_schema(neo4j_config=neo4j_config)
