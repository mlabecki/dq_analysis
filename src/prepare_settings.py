import os
import yaml

class ReadYaml:

    def __init__(self):
        
        self.src_dir = os.getcwd()
        self.root_dir = os.path.join(self.src_dir, '..')
        self.freddie_mac_dir = os.path.join(self.root_dir, 'data/freddie_mac/standard')
        self.config_file = os.path.join(self.root_dir, 'cfg', 'freddie_mac.yaml')
        self.conf = yaml.safe_load(open(self.config_file))

        self.layout_file = self.conf['layout_file']
        self.quarterly_file_prefix = self.conf['quarterly_file_prefix']
        self.monthly_file_prefix = self.conf['monthly_file_prefix']
        self.dq_subdir = self.conf['dq_subdir']
        self.min_extract_date = self.conf['min_extract_date']
        self.max_extract_date = self.conf['max_extract_date']

        self.datecol = self.conf['datecol']
        self.loancol = self.conf['loancol']
        
