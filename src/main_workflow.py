from mapping_freddie_mac import *
from prepare_data import FreddieMac
import sys


class Workflow:

    def __init__(self):
        self.varname = self.get_varname()
        if self.varname is None:
            self.varname = 'Estimated LTV'
        self.vartype = varname_type_map[self.varname]


    def get_varname(self):

        varname_dict = {}
        n = len(varname_type_map)
        input_text = f'Pick a number from 1 to {n} to select a variable from the following list:\n'
        for i, varname in enumerate(varname_type_map.keys()):
            input_text += '\t' + str(i + 1) + ': ' + varname + '\n'
            varname_dict.update({i + 1: varname})
        input_text += '\n'

        varname = ''
        while varname == '':
            ans = input(input_text)
            if ans.isdigit():
                if int(ans) not in range(1, n + 1):
                    print('Incorrect input, please try again:')
                else:
                    varname = varname_dict[int(ans)]
                    print(f'You selected {varname} as your variable.')
            else:
                print('Invalid input, expecting a number.')

        return varname


    def main(self):

        # freddie_mac = FreddieMac()
        # freddie_mac.reformat_original_data()
        # freddie_mac.extract_monthly_by_month(varname, vartype)

        print(self.varname, self.vartype)


wf = Workflow()
wf.main()
