import os
import pandas as pd
from pathlib import Path

from h5plexos.query import PLEXOSSolution


def convert_solution_files_to_h5(self):
    """
    Converts solution files in ZIP format to H5 format using the H5PLEXOS Julia library.
    Does that automatically for all ZIP files in the subdirectory "04_SolutionFiles/model_name" in the given main
    directory. The H5 files are saved in the same directory. Existing H5 files are not overwritten, but skipped.
    Ensure that Julia and the H5PLEXOS library are installed and are accessible in your environment.
    """
    # todo maybe also allow .zips in nested folders to be converted
    from julia.api import Julia

    jl = Julia(compiled_modules=False)
    jl.using("H5PLEXOS")

    soln_zip_files = [f for f in os.listdir(self.DIR_04_SOLUTION_FILES)
                      if f.endswith('.zip')]

    missing_soln_files = [f.split('.')[0] for f in soln_zip_files if f.replace('.zip', '.h5')
                          not in os.listdir(self.DIR_04_SOLUTION_FILES)]

    print(f'Found {len(missing_soln_files)} missing h5 files. Starting conversion...')
    for h5_file in missing_soln_files:
        jl.eval("cd(\"{}\")".format(self.DIR_04_SOLUTION_FILES.replace('\\', '/')))
        jl.eval("process(\"{}\", \"{}\")".format(f'{h5_file}.zip', f'{h5_file}.h5'))

def get_object(path, timescale, object):
    """
    Retrieves a specific object based on the provided timescale.
    Does that by looping through all solution files (.h5) files in the 04_SolutionFiles folder and combining the
    data for the specified object in a single DataFrame. The DataFrame is then processed very minimally to make it
    more readable and usable. To allow for huge data sets, the data is processed and returned using Dask DataFrames.

    Parameters:
    timescale (str): The timescale to use when retrieving the object. Either 'interval' or 'year'.
    object (str): The name of the object to retrieve. E.g., 'nodes', 'generators', 'regions'.

    Returns:
    dd.DataFrame: The retrieved data.

    Raises:
    ValueError: If no data is found for the specified object in any of the solution files.
    """
    if timescale not in ['interval', 'year']:
        raise ValueError('type must be either "interval" or "year"')


    with PLEXOSSolution(path) as db:

        properties = list(db.h5file[f'data/ST/{timescale}/{object}/'].keys())

        for obj_prop in properties:

            # Relations (i.e. membership-related props) have underscores in them
            if '_' not in object:
                db_data = db.query_object_property(
                    object_class=object[:-1],  # object class is queried without the 's' at the end of its name
                    # (this may be a bug that is corrected in future versions)
                    prop=obj_prop,
                    timescale=timescale,
                    phase="ST").reset_index(),

            else:
                db_data = db.query_relation_property(
                    relation=object,
                    prop=obj_prop,
                    timescale=timescale,
                    phase="ST").reset_index(),
            if len(db_data) == 1:
                db_data = db_data[0]
            else:
                raise ValueError('Multiple dataframes returned for {} {}'.format(object, obj_prop))

        return db_data


def get_two_part_SRMC_calculations(sourceDirMain, sourceDirSRMC, SaveDir, refscensMain, refscensSRMC, regionAsNode=True, auxgen=True):
    nodequery = "Region"
    totalquery = "Zone"
    if not regionAsNode:
        nodequery = "Node"
        totalquery = "Region"

    csvwd = Path(SaveDir) / "twoPartSRMCcalcs"
    csvwd.mkdir(parents=True, exist_ok=True)

    daily_all_gens = pd.DataFrame()
    annual_all_gens = pd.DataFrame()
    annual_tech_gens = pd.DataFrame()
    daily_all_bats = pd.DataFrame()
    annual_all_bats = pd.DataFrame()
    annual_tech_bats = pd.DataFrame()

    for i in range(len(refscensMain)):
        scenMain = refscensMain[i]
        scenSRMC = refscensSRMC[i]

        # # Navigate to sourceDirMain directory
        # os.chdir(sourceDirMain)
        # dbMain = plexos_open(str(scenMain))
        #
        #
        #
        # # Navigate to sourceDirSRMC directory
        # os.chdir(sourceDirSRMC)
        # dbSRMC = plexos_open(str(scenSRMC))
        #
        # # Set working directory to sourceDirSRMC
        # os.chdir(sourceDirSRMC)

        # Try to add time separators to the SRMC query
        df = get_object(path=sourceDirMain / f'{refscensSRMC[i]}.h5', timescale='year', object=totalquery)

        SRMCTSz = add_time_separators(query_interval(dbSRMC, totalquery, "SRMC"), 'time')
        SRMCTSz['reg_SRMC'] = SRMCTSz['value']
        SRMCTSz['scen_SRMC'] = SRMCTSz['scenario']


        # Add time separators to the Price query
        priceTS = add_time_separators(query_interval(dbSRMC, nodequery, "Price"), 'time')
        priceTS['Region'] = priceTS['name']
        priceTS['Price'] = priceTS['value']

        # Change working directory to sourceDirMain
        os.chdir(sourceDirMain)

        # Query the database for generator data
        if auxgen:
            GenTS = add_time_separators(simple_index_scen_type(query_interval(dbMain, "Generator", "Generation Sent Out")),
                                        'time')
            GenTS['property'] = "Generation"
        else:
            GenTS = add_time_separators(simple_index_scen_type(query_interval(dbMain, "Generator", "Generation")), 'time')


        srmc_rev = GenTS.merge(SRMCTSz[['scen_SRMC', 'time', 'reg_SRMC']], how='left')
        both_rev = srmc_rev.merge(priceTS[['Region', 'time', 'Price']], how='left')

        both_rev['SRMC_revenue'] = both_rev['value'] * both_rev['reg_SRMC'] / 1000
        both_rev['Price_rev_NOC'] = both_rev['value'] * both_rev['Price'] / 1000

        # Aggregate data
        srmc_rev_day = both_rev[["scenario", "scen_SRMC", "name", "date", "SRMC_revenue", "Price_rev_NOC"]].groupby(
            ["scenario", "scen_SRMC", "name", "date"]).sum().reset_index()

        # Convert date to datetime
        srmc_rev_day['time'] = pd.to_datetime(srmc_rev_day['date'], format="%Y-%m-%d")

        # Get also charging load and costs for PSH
        PSHld = add_time_separators(simple_index_scen_type(query_interval(dbMain, "Generator", "Pump Load")), 'time')
        srmc_pcst = PSHld.merge(SRMCTSz[['scen_SRMC', 'time', 'reg_SRMC']], how='left')
        both_pcst = srmc_pcst.merge(priceTS[['Region', 'time', 'Price']], how='left')

        both_pcst['SRMC_pumpcost'] = both_pcst['value'] * both_pcst['reg_SRMC'] / 1000
        both_pcst['Price_pumpcost_NOC'] = both_pcst['value'] * both_pcst['Price'] / 1000

        # Aggregate data
        srmc_pcst_day = both_pcst[["scenario", "scen_SRMC", "name", "date", "SRMC_pumpcost", "Price_pumpcost_NOC"]].groupby(
            ["scenario", "scen_SRMC", "name", "date"]).sum().reset_index()
        # Convert date to datetime
        srmc_pcst_day['time'] = pd.to_datetime(srmc_pcst_day['date'], format="%Y-%m-%d")

        # Navigate to sourceDirMain directory
        os.chdir(sourceDirMain)
        dbMain = plexos_open(str(scenMain))

        # Query the database for different properties
        pr = add_time_separators(simple_index_scen_type(query_day(dbMain, "Generator", "Pool Revenue")), 'time')
        netrev = add_time_separators(simple_index_scen_type(query_day(dbMain, "Generator", "Net Revenue")), 'time')
        nrr = add_time_separators(simple_index_scen_type(query_day(dbMain, "Generator", "Net Reserves Revenue")), 'time')
        genc = add_time_separators(simple_index_scen_type(query_day(dbMain, "Generator", "Generation Cost")), 'time')
        emc = add_time_separators(simple_index_scen_type(query_day(dbMain, "Generator", "Emissions Cost")), 'time')
        puc = add_time_separators(simple_index_scen_type(query_day(dbMain, "Generator", "Pump Cost")), 'time')
        stc = add_time_separators(simple_index_scen_type(query_day(dbMain, "Generator", "Start & Shutdown Cost")), 'time')

        # Combine all dataframes
        revcalc = pd.concat([pr, netrev, nrr, genc, emc, puc, stc])

        # Reshape the dataframe from long to wide format
        revcalc2 = revcalc.pivot_table(index=['scenario', 'name', 'time'], columns='property', fill_value=0).reset_index()

        # Calculate a new column "profit_price_OC"
        revcalc2['profit_price_OC'] = revcalc2['Pool Revenue'] + revcalc2['Net Reserves Revenue'] - revcalc2[
            'Generation Cost'] - revcalc2['Emissions Cost'] - revcalc2['Pump Cost'] - revcalc2['Start & Shutdown Cost']

        # Merge dataframes
        revcalc3 = pd.merge(revcalc2, srmc_rev_day[['scen_SRMC', 'name', 'time', 'SRMC_revenue', 'Price_rev_NOC']],
                            how='left', on=['scen_SRMC', 'name', 'time'])
        revcalc4 = pd.merge(revcalc3, srmc_pcst_day[['scen_SRMC', 'name', 'time', 'SRMC_pumpcost', 'Price_pumpcost_NOC']],
                            how='left', on=['scen_SRMC', 'name', 'time'])

        # Replace NA values with 0
        revcalc4[['SRMC_pumpcost', 'Price_pumpcost_NOC']] = revcalc4[['SRMC_pumpcost', 'Price_pumpcost_NOC']].fillna(0)

        # Create new columns
        revcalc4['profit_SRMC_NOC'] = revcalc4['SRMC_revenue'] + revcalc4['Net Reserves Revenue'] - revcalc4[
            'Generation Cost'] - revcalc4['Emissions Cost'] - revcalc4['SRMC_pumpcost'] - revcalc4['Start & Shutdown Cost']
        revcalc4['profit_price_NOC'] = revcalc4['Price_rev_NOC'] + revcalc4['Net Reserves Revenue'] - revcalc4[
            'Generation Cost'] - revcalc4['Emissions Cost'] - revcalc4['Price_pumpcost_NOC'] - revcalc4[
                                           'Start & Shutdown Cost']

        # Calculate uplift
        revcalc4['uplift_SRMC_NOC'] = revcalc4['profit_SRMC_NOC'] * -1
        revcalc4.loc[revcalc4['uplift_SRMC_NOC'] < 0, 'uplift_SRMC_NOC'] = 0
        revcalc4['uplift_price_NOC'] = revcalc4['profit_price_NOC'] * -1
        revcalc4.loc[revcalc4['uplift_price_NOC'] < 0, 'uplift_price_NOC'] = 0

        # Calculate profit with uplift
        revcalc4['profit_SRMC_NOC_uplift'] = revcalc4['profit_SRMC_NOC'] + revcalc4['uplift_SRMC_NOC']
        revcalc4['profit_price_NOC_uplift'] = revcalc4['profit_price_NOC'] + revcalc4['uplift_price_NOC']

        # Aggregate data
        rev_annual = revcalc4.groupby(['scenario', 'scen_SRMC', 'name']).sum().reset_index()

        # Append data to existing dataframes
        daily_all_gens = daily_all_gens.append(revcalc4)
        annual_all_gens = annual_all_gens.append(rev_annual)

        # Add installed capacity
        inst_cap = add_time_separators(simple_index_scen_type(query_year(dbMain, "Generator", "Installed Capacity")),
                                       'time')

        # Merge with rev_annual dataframe
        revenue_by_cap_pl = pd.merge(inst_cap[['scenario', 'name', 'Subtype', 'property', 'value']],
                                     rev_annual[
                                         ['scen_SRMC', 'name', 'profit_price_OC', 'profit_price_NOC', 'profit_SRMC_NOC',
                                          'profit_price_NOC_uplift', 'profit_SRMC_NOC_uplift']],
                                     on=['scen_SRMC', 'name'])

        # Add new column 'installed'
        revenue_by_cap_pl['installed'] = revenue_by_cap_pl['value']

        # Aggregate data by scenario, scen_SRMC and Subtype
        revenue_by_cap_tech = revenue_by_cap_pl.groupby(['scenario', 'scen_SRMC', 'Subtype']).sum().reset_index()

        # Calculate profits per MW by technology
        revenue_by_cap_tech['profit_Price_perMW_OC'] = revenue_by_cap_tech['profit_price_OC'] / revenue_by_cap_tech[
            'installed']
        revenue_by_cap_tech['profit_Price_perMW_NOC'] = revenue_by_cap_tech['profit_price_NOC'] / revenue_by_cap_tech[
            'installed']
        revenue_by_cap_tech['profit_SRMC_perMW_NOC'] = revenue_by_cap_tech['profit_SRMC_NOC'] / revenue_by_cap_tech[
            'installed']
        revenue_by_cap_tech['profit_Price_uplift_perMW_NOC'] = revenue_by_cap_tech['profit_price_NOC_uplift'] / \
                                                               revenue_by_cap_tech['installed']
        revenue_by_cap_tech['profit_SRMC_uplift_perMW_NOC'] = revenue_by_cap_tech['profit_SRMC_NOC_uplift'] / \
                                                              revenue_by_cap_tech['installed']

        # Append data to existing dataframe
        annual_tech_gens = pd.concat([annual_tech_gens, revenue_by_cap_tech])


        # Set working directory
        os.chdir(sourceDirMain)

        # Query the database for battery data
        batld = add_time_separators(simple_index_scen_type(query_interval(dbMain, "Battery", "Load")), 'time')
        batgen = add_time_separators(simple_index_scen_type(query_interval(dbMain, "Battery", "Generation")), 'time')

        # Combine and reshape the data
        batldgen = pd.concat([batld, batgen])
        batldgen = batldgen.pivot_table(index=['scenario', 'name', 'Region', 'time', 'date'], columns='property',
                                        fill_value=0).reset_index()

        # Merge with SRMCTSz and priceTS dataframes
        batSRMC = pd.merge(batldgen, SRMCTSz[['scen_SRMC', 'time', 'reg_SRMC']], on=['scen_SRMC', 'time'], how='left')
        batSRMC = pd.merge(batSRMC, priceTS[['Region', 'time', 'Price']], on=['Region', 'time'], how='left')

        # Perform calculations
        batSRMC['SRMCgenrev'] = batSRMC['Generation'] * batSRMC['reg_SRMC'] / 1000
        batSRMC['SRMCldcost'] = batSRMC['Load'] * batSRMC['reg_SRMC'] / 1000
        batSRMC['Pricegenrev'] = batSRMC['Generation'] * batSRMC['Price'] / 1000
        batSRMC['Priceldcost'] = batSRMC['Load'] * batSRMC['Price'] / 1000

        # Aggregate data
        batSRMC_day = batSRMC[
            ['scenario', 'scen_SRMC', 'name', 'date', 'SRMCgenrev', 'SRMCldcost', 'Pricegenrev', 'Priceldcost']].groupby(
            ['scenario', 'scen_SRMC', 'name', 'date']).sum().reset_index()
        batSRMC_day['time'] = pd.to_datetime(batSRMC_day['date'], format="%Y-%m-%d")

        # Query the database for different properties
        batrrev = add_time_separators(simple_index_scen_type(query_day(dbMain, "Battery", "Reserves Revenue")), 'time')
        batgrev = add_time_separators(simple_index_scen_type(query_day(dbMain, "Battery", "Generation Revenue")), 'time')
        batprof = add_time_separators(simple_index_scen_type(query_day(dbMain, "Battery", "Net Profit")), 'time')
        batnrev = add_time_separators(simple_index_scen_type(query_day(dbMain, "Battery", "Net Generation Revenue")),
                                      'time')
        batcost = add_time_separators(simple_index_scen_type(query_day(dbMain, "Battery", "Cost to Load")), 'time')

        # Combine dataframes
        batgrev = pd.concat([batrrev, batgrev])
        batp = pd.concat([batgrev, batcost, batprof, batnrev])

        # Reshape the dataframe from long to wide format
        batpcalc = batp.pivot_table(index=['name', 'time', 'scenario'], columns='property', fill_value=0).reset_index()

        # Aggregate data
        revenue_bat_ag = batpannual2[
            ['scenario', 'scen_SRMC', 'installed', 'profit_price_OC', 'profit_price_NOC', 'profit_SRMC_NOC']].groupby(
            ['scenario', 'scen_SRMC']).sum().reset_index()

        # Calculate new columns
        revenue_bat_ag['profit_Price_perMW_OC'] = revenue_bat_ag['profit_price_OC'] / revenue_bat_ag['installed']
        revenue_bat_ag['profit_SRMC_perMW_NOC'] = revenue_bat_ag['profit_SRMC_NOC'] / revenue_bat_ag['installed']
        revenue_bat_ag['profit_Price_perMW_NOC'] = revenue_bat_ag['profit_price_NOC'] / revenue_bat_ag['installed']

        # Append data to existing dataframe
        annual_tech_bats = pd.concat([annual_tech_bats, revenue_bat_ag])

    daily_all_gens.to_csv(os.path.join(csvwd, "01a_daily_all_generators.csv"), index=False)
    annual_all_gens.to_csv(os.path.join(csvwd, "01b_annual_all_generators.csv"), index=False)
    annual_tech_gens.to_csv(os.path.join(csvwd, "01c_annual_generators_aggregated_by_tech.csv"), index=False)
    daily_all_bats.to_csv(os.path.join(csvwd, "02a_daily_all_batteries.csv"), index=False)
    annual_all_bats.to_csv(os.path.join(csvwd, "02b_annual_all_batteries.csv"), index=False)
    annual_tech_bats.to_csv(os.path.join(csvwd, "02c_annual_batteries_aggregated.csv"), index=False)

    # Assign "Battery" to the "Subtype" column in the dataframe annual_tech_bats
    annual_tech_bats['Subtype'] = "Battery"

    # Append annual_tech_bats to annual_tech_gens
    annual_tech_gens = annual_tech_gens.append(annual_tech_bats)

    # Reshape the dataframe annual_tech_gens into a wide format using pivot_table
    SRMC_NOC_profit_MW_table = annual_tech_gens.pivot_table(index=['scenario', 'scen_SRMC'], columns='Subtype',
                                                            values='profit_SRMC_perMW_NOC').reset_index()

    Price_NOC_profit_MW_table = annual_tech_gens.pivot_table(index=['scenario', 'scen_SRMC'], columns='Subtype',
                                                             values='profit_Price_perMW_NOC').reset_index()

    Price_OC_profit_MW_table = annual_tech_gens.pivot_table(index=['scenario', 'scen_SRMC'], columns='Subtype',
                                                            values='profit_Price_perMW_OC').reset_index()

    SRMC_NOC_profit_uplift_MW_table = annual_tech_gens.pivot_table(index=['scenario', 'scen_SRMC'], columns='Subtype',
                                                                   values='profit_SRMC_uplift_perMW_NOC').reset_index()

    Price_NOC_profit_uplift_MW_table = annual_tech_gens.pivot_table(index=['scenario', 'scen_SRMC'], columns='Subtype',
                                                                    values='profit_Price_uplift_perMW_NOC').reset_index()

    # Write dataframes to CSV files
    SRMC_NOC_profit_MW_table.to_csv(os.path.join(csvwd, "03a_revenue_per_MW_SRMC_NOC.csv"), index=False)
    Price_NOC_profit_MW_table.to_csv(os.path.join(csvwd, "03b_revenue_per_MW_Price_NOC.csv"), index=False)
    Price_OC_profit_MW_table.to_csv(os.path.join(csvwd, "03c_revenue_per_MW_Price_OC.csv"), index=False)

    SRMC_NOC_profit_uplift_MW_table.to_csv(os.path.join(csvwd, "03d_revenue_per_MW_SRMC_uplift_NOC.csv"), index=False)
    Price_NOC_profit_uplift_MW_table.to_csv(os.path.join(csvwd, "03e_revenue_per_MW_Price_uplift_NOC.csv"), index=False)


if __name__ == "__main__":
    from pathlib import Path
    sourcewd = Path('U:/data/Korea/Market design 2021/04_SolutionFiles/2023_06_02_main_reproc')
    savewd = Path('S:/Korea/Market design 2021/05_DataProcessing/2023_06_02_main_reproc')

    models = ['Model Korea_2020_valid_mins Solution', 'Model Korea_2034_BPLE_mins Solution',
              'Model Korea_2035_WEO_APS_flex Solution', 'Model Korea_2035_WEO_APS_flex_nCO2 Solution']

    get_two_part_SRMC_calculations(
        sourceDirMain=sourcewd,
        sourceDirSRMC=sourcewd,
        SaveDir=Path(os.getcwd()),
        refscensMain=models,
        refscensSRMC=models)

