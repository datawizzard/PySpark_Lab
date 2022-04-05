import os
settings = {
    'host':os.environ.get('ACCOUNT_HOST','https://anand-21.documents.azure.com:443/'),
    'master_key':os.environ.get('ACCOUNT_KEY','lNWavbuybcBYUXMeW27EBYfINjtxaMzNfUXxH3JaFGOgmh6sqepzcDShzX1OjO2UHIfX5BwdU0OcsJLw3QbaRw=='),
    'database_id':os.environ.get('COSMOS_DATABASE','ToDoList'),
    'container_id':os.environ.get('COSMOS_CONTAINER','Items')
}