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

    #######################
    # UKRAINE MODEL NAMES #
    #######################
    
    'UKR_2021_Validation' : '2021',
    "UKR_2023_Td": "2023",
    
    "UKR_2025-BAU_Td": "2025",
    "UKR_2025-BAU_ST_LT_eSG_Td_CO2": "2025_eSG",
    "UKR_2025-BAU_ST_LT_eG_Td_CO2": "2025_eG",
    "UKR_2025-BAU_ST_LT_eSBG_Td_CO2": "2025_eSBG",
    "UKR_2025-BAU_ST_LT_eSG_Td_CO2_BTo": "2025_eSG-BTo",


    "UKR_2030-BR_cPN_CO2": "2030-BR_cPN",
    "UKR_2030-BR_cPN_eSG_ST_LT_CO2": "2030-BR_cPN_eSG",
    "UKR_2030-BR_cPN_eSG_ST_LT_CO2_ASb": "2030-BR_cPN_eSG_ASb",
    "UKR_2030-BR_cPN_eSG_ST_LT_CO2_ICc": "2030-BR_cPN_eSG_ICc",
    "UKR_2030-BR_cPN_eSG_ST_LT_CO2_nBTo": "2030-BR_cPN_eSG_nBTo",
    "UKR_2030-BR_cPN_eSBGW_ST_LT_CO2": "2030-BR_cPN_eSBGW",

    "UKR_2030-BAU_cPN_eSG_ST_LT_CO2": "2030-BAU_cPN_eSG",
    "UKR_2030-IR_cPN_eSG_ST_LT_CO2": "2030-IR_cPN_eSG",
    "UKR_2030-BR_cO_eSG_ST_LT_CO2": "2030-BR_cO_eSG",
    "UKR_2030-BR_cON_eSG_ST_LT_CO2": "2030-BR_cON_eSG",
    "UKR_2030-BR_cP_eSG_ST_LT_CO2": "2030-BR_cP_eSG",
        

    ######################
    # CHINA MODEL NAMES #
    ######################

    "01_China_2022_nMUT_FD_IRc":"2022",  
   
    "02_China_APS_2030_ED_nMUT":"2030_APS-ED",
    "02_China_APS_2030_FDp_nMUT":"2030_APS-FDp",
    "02_China_APS_2030_FDp_nMUT_IRc":"2030_APS-FDp-IRc",
    "02_China_APS_2030_FDp_nMUT_nDSM":"2030_APS-FDp_nDSM",

    "02_China_APS_2030_ED_rem_links_nMUT":"2030_APS-ED-nIC",
    "02_China_APS_2030_FD_rem_links_nMUT":"2030_APS-FD-nIC",
    "02_China_APS_2030_FDp_ASb_rem_links_nMUT":"2030_APS-FDp-ASb-nIC",
    "02_China_APS_2030_FDp_nBO_rem_links_nMUT":"2030_APS-FDp-nBO-nIC",
    "02_China_APS_2030_FDp_rem_links_nMUT_nBAS":"2030_APS-FDp-nIC-nBAS",
    "02_China_APS_2030_FDp_rem_links_nMUT_nDSM":"2030_APS-FDp-nIC-nDSM",

    "02_China_APS_2030_ED_UCset_nMUT":"2030_APS-ED-SM",
    "02_China_APS_2030_FD_UCset_nMUT":"2030_APS-FD-SM",
    "02_China_APS_2030_FDp_ASb_UCset_nMUT":"2030_APS-FDp-ASb-SM",
    "02_China_APS_2030_FDp_ASbd_UCset_nMUT":"2030_APS-FDp-ASf-SM",
    "02_China_APS_2030_FDp_ASd_UCset_nMUT":"2030_APS-FDp-ASd-SM",
    "02_China_APS_2030_FDp_nBO_UCset_nMUT":"2030_APS-FDp-nBO-SM",
    "02_China_APS_2030_FDp_UCset_nMUT_nBAS":"2030_APS-FDp-SM_nBAS",
    "02_China_APS_2030_FDp_UCset_nMUT_nDSM":"2030_APS-FDp-SM_nDSM",


    ### older
    "02_China_APS_2030_VPP_FDp_BTMopt_nMUT":"2030_APS_VPP-FDp-BTMo",
    "02_China_APS_2030_VPP_FDp_BTMoptAS_DERsAS_nMUT":"2030_APS_VPP-FDp-ASf",
    "02_China_APS_2030_VPP_FDp_BTMoptAS_nMUT":"2030_APS_VPP-FDp-ASb",
    "02_China_APS_2030_VPP_FDp_DERsAS_nMUT":"2030_APS_VPP-FDp-ASd",
    "02_China_APS_2030_VPP_FDp_nMUT":"2030_APS_VPP-FDp",
    "02_China_APS_2030_VPPext_FDp_nMUT":"2030_APS_VPPx-FDp",


    "02_China_APS_2030_FDp_rem_links_nMUT":"2030_APS-FDp-nIC",
    "02_China_APS_2030_FDp_UCset_nMUT":"2030_APS-FDp-SM",
    "02_China_APS_2030_VPP_FDp_rem_links_nMUT":"2030_APS_VPP-FDp-nIC",
    "02_China_APS_2030_VPP_FDp_UCset_nMUT":"2030_APS_VPP-FDp-SM",
    "02_China_APS_2030_VPPext_FDp_rem_links_nMUT":"2030_APS_VPPx-FDp_nIC",
    "02_China_APS_2030_VPPext_FDp_UCset_nMUT":"2030_APS_VPPx-FDp-SM",

    "02_China_APS_2030_VPP_FDp_UCset_nMUT_nBAS":"2030_APS_VPP-FDp-SM_nBAS",
    "02_China_APS_2030_VPP_FDp_rem_links_nMUT_nBAS":"2030_APS_VPP-FDp-nIC_nBAS",
    "02_China_APS_2030_VPP_FDp_nMUT_nBAS":"2030_APS_VPP-FDp_nBAS",
    "02_China_APS_2030_FDp_nMUT_nBAS":"2030_APS-FDp_nBAS",


}

FILTER_OUT_OBJS = ['constraints', 'contingencyreserves_regions', 'decisionvariables', 'storages', 'timeslices',
                   'variables']

### Traditionally, this was only used for timeseries DFs. This may need editing for certain outputs (to see)
### Otherwise, we need to check whether its an interval DF or not, and then filter or not
FILTER_PROPS = {
    'generators': ['Available Capacity', 'Generation', 'Installed Capacity', 'Min Energy Violation', 'Units',
                   'Units Generating', 'Units Out', 'Units Started', 'Forced Outage', 'Maintenance', 'Firm Capacity'],
    'batteries': ['Age', 'Energy', 'Generation', 'Hours Charging', 'Hours Discharging', 'Hours Idle',
                  'Installed Capacity', 'Generation Capacity', 'Load', 'Losses', 'Lower Reserve', 'Net Generation', 'Raise Reserve',
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

PLOTS_ALL_INCL = ['load_by_reg', 'use_by_reg', 'gen_by_tech', 'gen_by_WEOtech', 'gen_by_reg', 
'net_gen_by_reg', 'vre_by_reg_byGen', 'vre_by_reg_byAv', 're_by_reg', 'pk_load_by_reg', 'pk_netload_by_reg', 'line_cap_reg', 
'line_net_exports_reg', 'line_exports_reg', 'line_imports_reg', 'fuels_by_type', 'fuels_by_subtype', 'co2_by_tech', 
'co2_by_fuels', 'co2_by_subfuels', 'co2_by_reg', 'co2_intensity_reg', 'curtailment_rate', 're_curtailed_by_tech', 
'gen_cap_by_reg', 'gen_cap_by_tech', 'gen_cap_by_WEOtech', 'cf_tech', 'cf_tech_transposed', 'op_costs_by_tech', 'op_costs_by_prop', 
'op_and_vio_costs_by_prop', 'tsc_by_tech', 'tsc_by_prop', 'lcoe_by_tech', 'lcoe_by_tech_T', 'ramp_pc_by_reg', 'th_ramp_pc_by_reg', 
'ramp_by_reg', 'th_ramp_by_reg', 'dsm_pk_contr']


SERVICE_TECH_IDX = {'Coal':'Coal',
'Gas-CCGT':'Gas',
'Gas-steam':'Gas',
'Gas-OCGT':'Gas',
'Fuel Cell':'Gas',
'Oil':'Oil',
'Coal (CCS)':'Coal',
'Gas (CCS)':'Gas',
'Nuclear':'Nuclear',
'Bioenergy':'Bioenergy & other renewables',
'Geothermal':'Geothermal',
'Solar':'Variable renewables',
'Other':'Bioenergy & other renewables',
'Bioenergy (CCS)':'Bioenergy & other renewables',
'Wind':'Variable renewables',
'Marine':'Bioenergy & other renewables',
'Hydro':'Hydro',
'PSH':'Storage',
'DSM':'Demand response',
'VRE':'Variable renewables',
'Battery':'Storage',
'Cofiring':'Coal',
'Battery_1h':'Storage',
'Battery_2h':'Storage',	
'Battery_4h':'Storage',	
'Battery_8h':'Storage',


#### TODO: NEW TECHS based on changes from China model. This should be the template for other models, and therefore other models should be updated

#"Battery":"Storage",
# Bioenergy
"Gas_CCGT":"Gas",
"Coal_IGCC":"Coal",
"Coal_Supercritical":"Coal",
"Coal_Subcritical":"Coal",
"Fuel_cell":"Gas",
"Gas_GT":"Gas",
"Gas_steam":"Gas",
# Geothermal
# Hydro
# Marine
# Nuclear
# Oil
"ConcentratedSolarPower":"Bioenergy & other renewables",
"SolarPV":"Variable renewables",
"DSMshift":"Demand response",
# Wind
# DSM

}

