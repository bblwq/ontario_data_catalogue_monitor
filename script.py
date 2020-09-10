from urllib import request
import json, csv, ssl
from datetime import datetime
import pytz
from dateutil.relativedelta import relativedelta

package_list_url = 'https://data.ontario.ca/api/3/action/package_list'
resource_metadata_base_url = 'https://data.ontario.ca/api/3/action/package_show?id='
today = datetime.now(pytz.timezone('America/Toronto')).replace(tzinfo=None)

with request.urlopen(package_list_url) as packages_getter:
    packages = json.loads(packages_getter.read().decode())["result"]
    pkg_count = len(packages)
    print('{} packages found.'.format(pkg_count))

with open('resources.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile)
    spamwriter.writerow(['title','license','access_level','keywords','last_validation_date','update_frequency','last_update_date','last_update_location','over_due_frequency_ratio','num_hosted_data_files','num_linked_data_files','num_supporting_files','organization','groups','as_of'])

    for i in range(pkg_count):
        if i % 10 == 0:
            print('Processing {} out of {} packages.'.format(i+1, pkg_count))

        with request.urlopen(resource_metadata_base_url + packages[i]) as resource_getter:
            resource = json.loads(resource_getter.read().decode())['result']
            attributes = [resource['title'], resource['license_title'], resource['access_level']]
            if resource['keywords']:
                attributes.append(', '.join(resource['keywords']['en']))
            else:
                attributes.append('')
            if 'current_as_of' in resource:
                attributes.append(resource['current_as_of'])
            else:
                attributes.append('')
            attributes.append(resource['update_frequency'])
            data_files = [file for file in resource['resources'] if 'type' in file and file['type']=='data']

            if data_files:
                last_updated = ''
                location = 'external_link'
                for data_file in data_files:
                    if 'data_last_updated' in data_file and data_file['data_last_updated'] >= last_updated:
                        last_updated = data_file['data_last_updated']
                        if isinstance(data_file['size'],int):
                            location = 'hosted'
                over_due = 0
                if last_updated:
                    attributes.extend([last_updated,location])
                    last_updated = datetime.strptime(last_updated, '%Y-%m-%d')

                    if resource['update_frequency'] == 'yearly':
                        over_due = (today-(last_updated+relativedelta(years=+1))).days/365
                    elif resource['update_frequency'] == 'biannually':
                        over_due = (today-(last_updated+relativedelta(months=+6))).days/183
                    elif resource['update_frequency'] == 'quarterly':
                        over_due = (today-(last_updated+relativedelta(months=+3))).days/91
                    elif resource['update_frequency'] == 'monthly':
                        over_due = (today-(last_updated+relativedelta(months=+1))).days/30
                    elif resource['update_frequency'] == 'weekly':
                        over_due = (today-(last_updated+relativedelta(weeks=+1))).days/7
                    elif resource['update_frequency'] == 'daily':
                        over_due = (today-(last_updated+relativedelta(days=+1))).days
                else:
                    attributes.extend(['',''])
                attributes.append(over_due if over_due > 0 else '')
            else:
                attributes.extend(['','',''])

            attributes.append(len([data_file for data_file in data_files if isinstance(data_file['size'],int)]))
            attributes.append(len([data_file for data_file in data_files if not isinstance(data_file['size'],int)]))
            attributes.append(len(resource['resources'])-len(data_files))
            attributes.append(resource['organization']['title'])
            if resource['groups']:
                attributes.append(', '.join([group['title'] for group in resource['groups']]))
            else:
                attributes.append('')
            attributes.append(today.strftime('%Y-%m-%d'))
            spamwriter.writerow(attributes)
