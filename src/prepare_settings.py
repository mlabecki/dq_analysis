import os
from mapping_freddie_mac import main_settings

class ReadSettings:

    def __init__(self):
        
        self.src_dir = os.getcwd()
        self.root_dir = os.path.join(self.src_dir, '..')
        self.freddie_mac_dir = os.path.join(self.root_dir, 'data/freddie_mac/standard')

        self.layout_file = os.path.join(self.root_dir, 'cfg', main_settings['layout_file'])
        self.quarterly_file_prefix = main_settings['quarterly_file_prefix']
        self.monthly_file_prefix = main_settings['monthly_file_prefix']
        self.dq_subdir = main_settings['dq_subdir']
        self.min_extract_date = main_settings['min_extract_date']
        self.max_extract_date = main_settings['max_extract_date']
        self.datecol = main_settings['datecol']
        self.loancol = main_settings['loancol']
        