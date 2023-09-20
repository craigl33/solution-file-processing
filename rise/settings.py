config_name = 'india_c.toml'


# 'C:/Users/TRIPPE_L/VM_Code/dev_dirs/Indonesia/2021_IPSE'
# 'Y:/RED/Modelling/Indonesia/2021_IPSE'
# '20230509_IDN_APSvRUPTL_scenario'
# 'C:/Users/TRIPPE_L/VM_Code/dev_dirs/Indonesia/2022_06_21_generator_parameters_IDN.xlsx'

# 'C:/Users/TRIPPE_L/Code/dev_dirs/Thailand/2022-3_next_step_modelling'
# 'Y:/RED/Modelling/Thailand/2022-3_next_step_modelling'
# '20230626_THA_Base_LowerCO2p'
# 'C:/Users/TRIPPE_L/Code/dev_dirs/Thailand/2023_03_14_generator_parameters_THA.xlsx'

import toml
import os

with open(os.path.join('configurations', config_name), 'r') as f:
    config = toml.load(f)

