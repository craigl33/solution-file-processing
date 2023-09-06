### As time-series data can be quite large, filter only certain data

FILTER_OUT_OBJS = ['constraints', 'contingencyreserves_regions', 'decisionvariables', 'storages', 'timeslices', 'variables']

FILTER_PROPS = {
    'generators': ['Available Capacity', 'Generation', 'Installed Capacity', 'Min Energy Violation', 'Units',
                   'Units Generating', 'Units Out', 'Units Started', 'Forced Outage', 'Maintenance', 'Firm Capacity'],
    'batteries': ['Age', 'Energy', 'Generation', 'Hours Charging', 'Hours Discharging', 'Hours Idle',
                  'Installed Capacity', 'Load', 'Losses', 'Lower Reserve', 'Net Generation', 'Raise Reserve',
                  'Regulation Lower Reserve', 'Regulation Raise Reserve', 'SoC'],
    'regions': ['Capacity Reserve Margin', 'Capacity Reserves', 'Customer Load', 'Dump Energy', 'Forced Outage',
                'Generation', 'Generation Capacity', 'Load', 'Maintenance', 'Native Load', 'Pump Load', 'Battery Load',
                'Transmission Losses', 'Unserved Energy', 'Unserved Energy Hours', 'Price', 'Shadow Price', 'SRMC'],
    # 'Exports', 'Imports'

    'nodes': ['Customer Load', 'Exports', 'Generation', 'Generation Capacity',
              'Imports', 'Load', 'Min Load', 'Native Load', 'Peak Load', 'Price', 'Pump Load', 'Battery Load',
              'Unserved Energy', 'Price', 'Shadow Price', 'SRMC'],
    'lines': ['Export Limit', 'Flow', 'Import Limit', 'Loading', 'Loss', 'Units'],

    # ## differ by emission_gens or plain emissions
    # filter_props['emissions_generators'] = ['Cost', 'Intensity', 'Generator Production', ]
    'emissions': ['Intensity', 'Price', 'Production', 'Shadow Price'],
    'fuels': ['Cost', 'Offtake', 'Price', 'Time-weighted Price'],
    'reserves': ['Shortage', 'Risk'],
    'reserves_generators': ['Available Response', 'Cleared Offer Cost', 'Cleared Offer Price',
                            'Non-spinning Reserve Provision',
                            'Provision']

}
