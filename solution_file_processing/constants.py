"""
TODO DOCSTRING
"""

VRE_TECHS = ['Solar', 'Wind']
CONSTR_TECHS = ['Hydro', 'Bioenergy', 'Geothermal']

PRETTY_MODEL_NAMES = {
    'IDN_2019_Validation': '2019',
    'IDN_2025_RUPTL': '2025 Base',
    'IDN_2025_RUPTL_EnforcedCofiring': '2025 Enforced Cofiring',
    'IDN_2025_RUPTL_NoCofiring': '2025 No Cofiring',
    'IDN_2025_RUPTL_SolPlus': '2025 SolarPlus',

    'IDN_2025_RUPTL_SolPlus2': '2025 SolarPlus Lite',
    'IDN_2025_RUPTL_SolPlus3': '2025 SolarPlus Extra',
    'IDN_2025_RUPTL_SolPlus_InflexIPP': '2025 SolarPlus InflexIPP',
    'IDN_2025_RUPTL_SolPlus_CC': '2025 SolarPlus - Clean Cooking',
    'IDN_2025_RUPTL_SolPlus_EVs': '2025 SolarPlus - EVs',
    'IDN_2025_RUPTL_SolPlus_NightEVs': '2025 SolarPlus - Controlled EVs',
    'IDN_2025_RUPTL_SolPlus_CC_EVs': '2025 SolarPlus - All New Loads',
    'IDN_2025_RUPTL_SolPlus_CC_NightEVs': '2025 SolarPlus - All New Loads & Controlled EVs',
    'IDN_2025_SDS': '2025 SDS',

    'IDN_2025_RUPTL_SolPlusExtra_CC_EVs_AnnTOP': '2025 SolarPlusExtra - Clean Cooking + EVs',
    'IDN_2025_RUPTL_SolPlus_ContrFlexC': '2025 SolarPlus - Coal Contractual Flexibility',
    'IDN_2025_RUPTL_SolPlus_ContrFlexG': '2025 SolarPlus - Gas Contractual Flexibility',
    'IDN_2025_RUPTL_SolPlus_ContrFlexCG': '2025 SolarPlus - Coal & Gas Contractual Flexibility',
    'IDN_2025_RUPTL_SolPlus_DryYear': '2025 SolarPlus - Dry Year',
    'IDN_2025_RUPTL_SolPlus_FullContrFlex': '2025 SolarPlus - Full Contractual Flexibility',

    'IDN_2030_RUPTL': '2030 Base',
    'IDN_2030_RUPTL_VRE_SDS': '2030 VREPlus',
    'IDN_2030_RUPTL_VRE_SDS_ToP': '2030 VREPlus - ToP',

    'IDN_2030_RUPTL_VRE_SDS_IC': '2030 VREPlus - IC',
    'IDN_2030_RUPTL_VRE_SDS_CC': '2030 VREPlus - Clean Cooking',
    'IDN_2030_RUPTL_VRE_SDS_EVs': '2030 VREPlus - EVs',
    'IDN_2030_RUPTL_VRE_SDS_NightEVs': '2030 VREPlus - Controlled EVs',
    'IDN_2030_RUPTL_VRE_SDS_CC_EVs': '2030 VREPlus - All New Loads',
    'IDN_2030_RUPTL_VRE_SDS_CC_NightEVs': '2030 VREPlus - All New Loads & Controlled EVs',
    'IDN_2030_RUPTL_VRE_SDS_CC_EVs_IC Solution': '2030 VREPlus - All New Loads & IC',
    'IDN_2030_RUPTL_VRE_SDS_CC_NightEVs_IC': '2030 VREPlus - All New Loads & Flexibility',
    'IDN_2030_SDS': '2030 SDS',
    'IDN_2030_SDS_EVs': '2030 SDS - EVs',
    'IDN_2030_SDS_NightEVs': '2030 SDS - Overnight Charging',
    'IDN_2030_SDS_SmartEVs': '2030 SDS - Smart Charging',

    'IDN_2040_NZE': '2040 NZE',
    'IDN_2040_NZE_w_DSM': '2040 NZE - DSM',
    'IDN_2040_NZE_CopperPlate': '2040 NZE - CP',
    'IDN_2040_NZE_Storage': '2040 NZE - Sto',
    'IDN_2040_NZE_CopperPlate_Storage': '2040 NZE - CP_Sto',
    'IDN_2040_NZE_DSM_wLTExp_FO-DSM': '2040 NZE - DSM_OptExp',
    'IDN_2040_NZE_wLTExp_WEOsplit': '2040 NZE WS - OptExp',
    'IDN_2040_NZE_WEOsplit': '2040 NZE WS',
    'IDN_2040_NZE_CopperPlate_Storage_WEOsplit': '2040 NZE WS - CP_Sto',
    'IDN_2040_NZE_DSM_wLTExp_FullOpt': '2040 NZE - DSM & FullOptExp',
    'IDN_2040_NZE_wLTExp': '2040 NZE',
    'IDN_2040_NZE_DSM_wLTExp': '2040 NZE - DSM',
    'IDN_2040_NZE_wLTExp_FullOpt': '2040 NZE - FullOptExp',

    ############################

    'IDN_2030_NZE_wLTExp': '2030 APS - No DSM',
    'IDN_2030_NZE_DSM_wLTExp': '2030 APS',
    'IDN_2030_NZE_DSM_wLTExp_CF65': '2030 APS - CF65',
    'IDN_2030_NZE_DSM_wLTExp_CF70': '2030 APS - CF70',

    'IDN_2030_NZE_wLTExp_FullOpt': '2030 APS - FullOptExp',
    'IDN_2030_NZE_DSM_wLTExp_ContrFlex': '2030 APS - Contractual Flexibility',

    'IDN_2050_NZE_wLTExp': '2050 APS - No DSM',
    'IDN_2050_NZE_DSM_wLTExp': '2050 APS',
    'IDN_2050_NZE_wLTExp_FullOpt': '2050 APS - FullOptExp',
    'IDN_2050_NZE_wLTExp_3dPM': '2050 APS - 3dExp',

    'IDN_2030_NZE_DSM_wLTExp_CO2': '2030 APS - CO2',
    'IDN_2030_NZE_DSM_wLTExp_ContrFlex_CO2': '2030 APS - CO2 & ContrFlex',

    'IDN_2050_NZE_DSM_wLTExp_2019 Solution': '2050 APS (2019)',
    'IDN_2050_NZE_DSM_wLTExp_2018 Solution': '2050 APS (2018)',
    'IDN_2050_NZE_DSM_wLTExp_2015 Solution': '2050 APS (2015)',
    'IDN_2050_NZE_DSM_wLTExp_2011 Solution': '2050 APS (2011)',
    'IDN_2050_NZE_DSM_wLTExp_2010 Solution': '2050 APS (2010)',

    'IDN_2030_NZE_DSM_wLTExp_2019 Solution': '2030 APS (2019)',
    'IDN_2030_NZE_DSM_wLTExp_2018 Solution': '2030 APS (2018)',
    'IDN_2030_NZE_DSM_wLTExp_2015 Solution': '2030 APS (2015)',
    'IDN_2030_NZE_DSM_wLTExp_2011 Solution': '2030 APS (2011)',
    'IDN_2030_NZE_DSM_wLTExp_2010 Solution': '2030 APS (2010)',

    'THA22_2021_Validation': '2021 Validation',
    'THA22_2025_Base': '2025 Base',

    'THA22_2030_Base': '2030 Base',
    'THA22_2030plus': '2030 VRE Plus',
    'THA22_2030plus_CO2p': '2030 VRE Plus + CO2p',

    'THA22_2030plus_PPflex': '2030 VRE Plus + PPFlex',
    'THA22_2030plus_Batt': '2030 VRE Plus + Sto',
    'THA22_2030plus_Batt_PPflex': '2030 VRE Plus + PPFlex + Sto',
    'THA22_2030plus_Batt_NoTOP_PPflex': '2030 VRE Plus + PPFlex + Sto + ContrFlex',

    'THA22_2037_Base': '2037 Base',
    'THA22_2037plus': '2037 VRE Plus',
    'THA22_2037plus_CO2p': '2037 VRE Plus + CO2p',
    'THA22_2037plus_Batt': '2037 VRE Plus + Sto',
    'THA22_2037plus_Batt_CO2p': '2037 VRE Plus + Sto + CO2p',
    'THA22_2037plus_EVFlex_Batt_PPflex': '2037 VRE Plus + PPFlex + Sto + EV',
    'THA22_2037plus_EVFlex_Batt_PPflex_CO2p': '2037 VRE Plus + PPFlex + Sto + EV + CO2p',

    'THA22_2037plus_PPflex': '2037 VRE Plus + PPFlex',
    'THA22_2037plus_EVFlex': '2037 VRE Plus + EV',
    'THA22_2037plus_EVFlex_Batt_NoTOP_PPflex': '2037 VRE Plus + PPFlex + Sto + EV + ContrFlex',

    'THA22_2030plus_Batt_NoTOP': '2030 VRE Plus + Sto + ContrFlex',
    'THA22_2037plus_EVFlex_Batt': '2037 VRE Plus + Sto + EV',
    'THA22_2037plus_EVFlex_Batt_NoTOP': '2037 VRE Plus + Sto + ContrFlex',
    "THA22_2037plus_EVFlex25": '2037 VRE Plus + EV25',
    "THA22_2037plus_EVFlex50": '2037 VRE Plus + EV50',
    "THA22_2037plus_Batt25": '2037 VRE Plus + Sto25',
    "THA22_2037plus_Batt50": '2037 VRE Plus + Sto50',
    "THA22_2037plus_EVFlex25_Batt_NoTOP_PPflex": '2037 VRE Plus + Sto + EV25 + ContrFlex',
    "THA22_2037plus_EVFlex25_Batt_PPflex": '2037 VRE Plus + Sto + EV25',
    "THA22_2037plus_EVFlex50_Batt_NoTOP_PPflex": '2037 VRE Plus + Sto + EV50 + ContrFlex',
    "THA22_2037plus_EVFlex50_Batt_PPflex": '2037 VRE Plus + Sto + EV50',

    ##########

    'UKR_2021_Validation' : '2021 (Validation)',
    'UKR_2023_Base_DamagedTx' : '2023 (Damage validation)',
    'UKR_2023_w_Load2021_DamagedTx' : '2023 with 2021 historical demand',
    'UKR_2025_BAU_DamagedTx' : '2025 (NZW BAU demand)',
    'UKR_2030_IR_Load-BAU_LT_ST_SolGas_CO2' : '2030 (NZW BAU demand)',
    'UKR_2030_IR_Load-BR_ST_LT_SolGas_CO2' : '2030 (NZW BR demand)',
    'UKR_2030_Load-BR_2021_Cap_ST_LT_SolGas_CO2' : '2030 (NZW BR demand) w/ 2021 capacity'

}

FILTER_OUT_OBJS = ['constraints', 'contingencyreserves_regions', 'decisionvariables', 'storages', 'timeslices',
                   'variables']

### Traditionally, this was only used for timeseries DFs. This may need editing for certain outputs (to see)
### Otherwise, we need to check whether its an interval DF or not, and then filter or not
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

    'nodes': ['Customer Load', 'Net DC Export', 'Exports', 'Generation', 'Generation Capacity',
              'Imports', 'Load', 'Min Load', 'Native Load', 'Peak Load', 'Price', 'Pump Load', 'Battery Load',
              'Unserved Energy', 'Price', 'Shadow Price', 'SRMC'],

    'zones': ['Customer Load', 'Exports', 'Net Interchange', 'Generation', 'Imports', 'Unserved Energy'],

    'lines': ['Export Limit', 'Flow', 'Flow Back', 'Import Limit', 'Loading', 'Loss', 'Units'],

    # ## differ by emission_gens or plain emissions
    # filter_props['emissions_generators'] = ['Cost', 'Intensity', 'Generator Production', ]
    'emissions': ['Intensity', 'Price', 'Production', 'Shadow Price'],
    'fuels': ['Cost', 'Offtake', 'Price', 'Time-weighted Price'],
    'reserves': ['Shortage', 'Risk'],
    'reserves_generators': ['Available Response', 'Cleared Offer Cost', 'Cleared Offer Price',
                            'Non-spinning Reserve Provision',
                            'Provision']

}