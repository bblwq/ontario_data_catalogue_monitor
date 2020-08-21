from urllib import request
import json, csv
from datetime import datetime
from dateutil.relativedelta import relativedelta

package_list_url = 'https://data.ontario.ca/api/3/action/package_list'
resource_metadata_base_url = 'https://data.ontario.ca/api/3/action/package_show?id='

with request.urlopen(package_list_url) as packages_getter:
    packages = json.loads(packages_getter.read().decode())["result"]
    pkg_count = len(packages)
    print('{} packages found.'.format(pkg_count))

with open('resources.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile)
    spamwriter.writerow(['title','license','access_level','exemption','exemption_rationale','keywords','last_validation_date','update_frequency','last_update_date','over_due_frequency_ratio','num_hosted_data_files','num_linked_data_files','num_supporting_files','organization','groups','geo_coverage','geo_breakdown'])
    
    for i in range(pkg_count):
        print('Processing {} out of {} packages.'.format(i+1, pkg_count))

        with request.urlopen(resource_metadata_base_url + packages[i]) as resource_getter:
            resource = json.loads(resource_getter.read().decode())['result']
            attributes = [resource['title'], resource['license_title'], resource['access_level']]
            if 'exemption' in resource:
                attributes.append(resource['exemption'])
            else:
                attributes.append('')
            attributes.append(resource['exemption_rationale']['en'])
            attributes.append(', '.join(resource['keywords']['en']))
            if 'current_as_of' in resource:
                attributes.append(resource['current_as_of'])
            else:
                attributes.append('')
            attributes.append(resource['update_frequency'])
            data_files = [file for file in resource['resources'] if 'type' in file and file['type']=='data']
            if data_files:
                today = datetime.today()
                last_updated = datetime.strptime(max([data_file['last_modified'][:10] if data_file['last_modified'] else data_file['created'][:10] for data_file in data_files]), '%Y-%m-%d')
                attributes.append(datetime.strftime(last_updated, '%Y-%m-%d'))
                over_due = 0
                if resource['update_frequency'] == 'yearly':
                    over_due = (today-(last_updated+relativedelta(years=+1))).days/365
                elif resource['update_frequency'] == 'biannually':
                    over_due = (today-(last_updated+relativedelta(months=+6))).days/183
                elif resource['update_frequency'] == 'quarterly':
                    over_due = (today-(last_updated+relativedelta(months=+3))).days/91
                elif resource['update_frequency'] == 'monthly':
                    over_due = (today-(last_updated+relativedelta(months=+1))).days/30
                elif resource['update_frequency'] == 'weekly':
                    over_due = (today-(last_updated+relativedelta(days=+1))).days/7
                elif resource['update_frequency'] == 'daily':
                    over_due = (today-(last_updated+relativedelta(days=+1))).days
                attributes.append(over_due if over_due > 0 else '')
            else:
                attributes.extend(['',''])
            attributes.append(len([data_file for data_file in data_files if isinstance(data_file['size'],int)]))
            attributes.append(len([data_file for data_file in data_files if not isinstance(data_file['size'],int)]))
            attributes.append(len(resource['resources'])-len(data_files))
            attributes.append(resource['organization']['title'])
            if resource['groups']:
                attributes.append(', '.join([group['title'] for group in resource['groups']]))
            else:
                attributes.append('')
            if resource['geographic_coverage_translated']:
                attributes.append(resource['geographic_coverage_translated']['en'])
            else:
                attributes.append('')
            if 'geographic_granularity' in resource:
                attributes.append(resource['geographic_granularity'])
            else:
                attributes.append('')
            spamwriter.writerow(attributes)