import os

### Set Environment Variables

os.environ['envn'] = 'PROD' # Testing Environment
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'


### Get Environment Variables

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']


## Set other Variables

appName = 'USA Prescriber Research Report'
current_path = os.getcwd()
#staging_dim_city = current_path + '\..\staging\dimension_city'
#staging_fact = current_path + '\..\staging\\fact'

staging_dim_city="PrescPipeline/staging/dimension_city"
staging_fact="PrescPipeline/staging/fact"

output_city="PrescPipeline/output/dimension_city"
output_fact="PrescPipeline/output/presc"


