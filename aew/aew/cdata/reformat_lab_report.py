
from gc import collect
from lib2to3.fixes.fix_except import find_excepts
from multiprocessing import Value
from pdb import find_function
from re import L
from turtle import color
import pandas as pd
import numpy as np
from io import BytesIO
from flask import send_file
import os


from .cdataRefs import *
from .cdataExcel import *




###### HELPER FUNCTIONS ######

def getDisplayName(aName):
    return analyte_name_to_display_name.get(aName)

def edd_validation(edd_file):
    # Rename FINALVALUE (Sometimes lab calls it that instead of FINALVAL)
    data = pd.read_excel(edd_file)
    data.rename(columns={"FINALVALUE":"FINALVAL"}, inplace=True)
    # Unify unit capitalization
    data["UNITS"].replace({"mg/kg":"mg/Kg"}, inplace=True)
    return data

def format_to_floats(str_int_or_float):
    string = str(str_int_or_float)
    clean_string = string.strip('< > *')
    if string == "NEG":
        return string
    if string == "POS":
        return string
    if '@' in string:
        return string
    if string == "ND":
        return string
    else:
        return float(clean_string)

def listdir_nohidden(path):
    for f in os.listdir(path):
        if not f.startswith("."):
            yield f


def pivot_analyte_group_data(analyte_group_df):
    #print("P1")
    #print(analyte_group_df["Analyte Group"].unique())
   
    # Deals with edge case where there is only one analyte and one sample(?) in an analyte group
    if isinstance(analyte_group_df, pd.Series):
        analyte_group_df = analyte_group_df.to_frame().transpose()
    #print("P2")
    return analyte_group_df.pivot(index="SAMPID", columns="ANALYTE", values="Results")


def transpose_analyte_group_units(analyte_group_df):
    analytes_units = analyte_group_df[["ANALYTE", "UNITS"]].drop_duplicates()
    analytes_units_transposed = analytes_units.transpose()
    # makes the column names be the 'References' instead of the index which
    # happens after transposing
    analytes_units_transposed.columns = analytes_units_transposed.loc["ANALYTE"]
    # gets rid of the first row which was the References, now no longer needed
    analytes_units_transposed.drop("ANALYTE", axis=0, inplace=True)
    return analytes_units_transposed


def get_analyte_group_from_testno(testno):
    if testno in analyte_groups_by_testno.keys():
        return analyte_groups_by_testno[testno]
    else:
        return testno


def get_GW_analyte_group_from_testno(testno):
    if testno in GW_analyte_groups_by_testno.keys():
        return GW_analyte_groups_by_testno[testno]
    else:
        return testno


def get_table1_group_from_testno(testno):
    if testno in table1_groups_by_testno.keys():
        return table1_groups_by_testno[testno]
    else:
        return testno


def get_addon_from_testcode(testcode):
    if "STLC" in testcode:
        return "WET "
    if "TCLP" in testcode:
        return "TCLP "
    else:
        return "Testcode Not Recognized"


def get_dry_from_units(units):
    if "dry" in units:
        return " Dry"
    else:
        return ""


def get_GW_from_testcode(row):
    if row["Analyte Group"] == "GW Metals":
        testcode = row["TESTCODE"]
        if "DISS" in testcode:
            return row["SAMPID"] + " (Dissolved)"
        if "TTLC" in testcode:
            return row["SAMPID"] + " (Total)"
        else:
            return "Testcode Not Recognized"
    else:
        return row["SAMPID"]


def drop_rows(analyte):
    if analyte in analytes_to_drop:
        return True
    else:
        return False


def drop_unwanted_analytes(lab_df):
    index_to_drop = lab_df[lab_df["ANALYTE"].apply(drop_rows)].index
    return lab_df.drop(index=index_to_drop).reset_index(drop=True)


def check_for_addon(testcode):
    if "STLC" in testcode or "TCLP" in testcode:
        return True
    else:
        return False


def check_for_gw(matrix):
    if "Water" in matrix:
        return True
    else:
        return False

def check_for_sv(matrix):
    if 'SoilGas' in matrix:
        return True
    else:
        return False


def get_analyte_display_name(analyte):
    if analyte in analyte_name_to_display_name.keys():
        return analyte_name_to_display_name[analyte]
    else:
        return analyte


def turn_NaN_to_NR(value):
    if pd.isnull(value):
        return "NR"
    else:
        return value


def turn_NaN_to_dash(value):
    if pd.isnull(value):
        return "- -"
    else:
        return value


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


def reorder_columns(df, column_order):
    # Check if any df columns are not in column order
    for column in df.columns:
        if column not in column_order:
            # Print a warning
            print(
                f'WARNING The column "{column}" is found in this df but not column_order list'
            )
            # Tack it onto the end of the order
            column_order.append(column)
    #           df[column]='NA'
    ordered_columns = intersection(column_order, df.columns)
    return df[ordered_columns]


def combine_data(df1, df2):
    df3 = pd.concat([df1, df2], sort=False)
    return df3.reset_index(drop=True)


# processing functions


def preprocess_all_data(lab_df):
    # Drop columns that are not relevant
    lab_df_clean_columns = lab_df.drop(
        columns=[
            "LABCODE",
            "RECEIVEDATE",
            "BATCHID",
            "CAS",
            "SAMPTYPE",
            "REC",
            "SPIKE",
            "DILFAC",
            "RPD",
            "LOWERCL",
            "UPPERCL",
            "FLAG"
            #       ,'MDL'
        ]
    )

    # ignores chemicals that are not results (i.e. surrogates)
    lab_df_clean_columns = lab_df_clean_columns[
        lab_df_clean_columns["ANALYTETYPE"] != "S"
    ]

    # Drop duplicates, keep row with most recent prepdate (for when samples are rerun with new standards)
    #    lab_df_no_duplicates = drop_duplicates(lab_df_clean_columns)

    # If EDD has column titled FINALVALUE, make FINALVAL
    lab_df_clean_columns.rename(columns={"FINALVALUE": "FINALVAL"}, inplace=True)

    # For PCBs: if dry is included in the units, add 'Dry' to the analyte name
    lab_df_clean_columns["ANALYTE"] = lab_df_clean_columns[
        "ANALYTE"
    ] + lab_df_clean_columns["UNITS"].apply(get_dry_from_units)

    # Add analyte group column
    lab_df_clean_columns["Analyte Group"] = lab_df_clean_columns["TESTNO"].apply(
        get_analyte_group_from_testno
    )

    processed_lab_data = lab_df_clean_columns

    return processed_lab_data


def preprocess_soil_lab_data(lab_df, comparison_data_soil):
    processed_soil_lab_data = preprocess_all_data(lab_df)
    # Add results column with RLs (Change NDs to RLs) and add in RL too high warnings
    processed_soil_lab_data["Results"] = processed_soil_lab_data.apply(
        format_results_with_rls,
        axis="columns",
        criteria_to_compare=comparison_data_soil,
    )
    # Add Table 1 Category column
    processed_soil_lab_data["Table1 Group"] = processed_soil_lab_data["TESTNO"].apply(
        get_table1_group_from_testno
    )
    return processed_soil_lab_data


def preprocess_lab_addon_data(lab_addon_df, comparison_data_soil):
    processed_lab_addon_data = preprocess_all_data(lab_addon_df)
    # Change analyte to add on test name
    processed_lab_addon_data["ANALYTE"] = (
        processed_lab_addon_data["TESTCODE"].apply(get_addon_from_testcode)
        + processed_lab_addon_data["ANALYTE"]
    )
    # Add results column with RLs (Change NDs to RLs) and add in RL too high warnings
    processed_lab_addon_data["Results"] = processed_lab_addon_data.apply(
        format_results_with_rls,
        axis="columns",
        criteria_to_compare=comparison_data_soil,
    )
    # Add Table 1 Category column
    processed_lab_addon_data["Table1 Group"] = processed_lab_addon_data["ANALYTE"]
    return processed_lab_addon_data


def preprocess_nonsoil_data(lab_nonsoil_df, comparison_data_nonsoil):
    # Drop columns that are not relevant
    lab_df_clean_columns = lab_nonsoil_df.drop(
        columns=[
            "LABCODE",
            "RECEIVEDATE",
            "BATCHID",
            "CAS",
            "SAMPTYPE",
            "REC",
            "SPIKE",
            "DILFAC",
            "RPD",
            "LOWERCL",
            "UPPERCL",
            "FLAG"
            #       ,'MDL'
        ]
    )

    # ignores chemicals that are not results (i.e. surrogates)
    lab_df_clean_columns = lab_df_clean_columns[
        lab_df_clean_columns["ANALYTETYPE"] != "S"
    ]

    # *the next 2 lines of code must come before the drop duplicate function for GW data*
    # Add analyte group column
    lab_df_clean_columns["Analyte Group"] = lab_df_clean_columns["TESTNO"].apply(
        get_GW_analyte_group_from_testno
    )

    # Change analyte to GW test name
    lab_df_clean_columns["SAMPID"] = lab_df_clean_columns.apply(
        get_GW_from_testcode, axis=1
    )

    # Drop duplicates, keep row with most recent prepdate (for when samples are rerun with new standards)
    #    lab_df_no_duplicates = drop_duplicates(lab_df_clean_columns)

    # If EDD has column titled FINALVALUE, make FINALVAL
    lab_df_clean_columns.rename(columns={"FINALVALUE": "FINALVAL"}, inplace=True)

    # Add results column with RLs (Change NDs to RLs) and add in RL too high warnings
    lab_df_clean_columns["Results"] = lab_df_clean_columns.apply(
        format_results_with_rls, axis="columns", criteria_to_compare=comparison_data_nonsoil
    )

    # Add Table 1 Category column
    lab_df_clean_columns["Table1 Group"] = lab_df_clean_columns["TESTNO"].apply(
        get_table1_group_from_testno
    )

    processed_nonsoil_data = lab_df_clean_columns

    return processed_nonsoil_data


def sort_non_soil_data(df):
    index_of_addon = df["TESTCODE"].apply(check_for_addon)
    index_of_gw = df["MATRIX"].apply(check_for_gw)
    index_of_sv = df['MATRIX'].apply(check_for_sv)
    index_of_non_soil = index_of_gw | index_of_sv
    # nonsoil and addon indexes are only the same when both are false. When both are false we have soil data.
    index_of_soil = index_of_addon == index_of_non_soil
           
    soil_df = df[index_of_soil].reset_index(drop=True)
    addon_df = df[index_of_addon].reset_index(drop=True)
    GW_df = df[index_of_gw].reset_index(drop=True)
    SV_df = df[index_of_sv].reset_index(drop=True)
    non_soil_df = df[index_of_non_soil].reset_index(drop=True)
    return soil_df, addon_df, GW_df, SV_df, non_soil_df


def generate_table_1(lab_data, sample_order_df):

    table1_columns = [#'LABSAMPID',
                      'SAMPID',
                      #'PROJNAME',
                      'SAMPDATE']
    table1_info = lab_data[table1_columns]
    table1_info = table1_info.drop_duplicates()
    table1_info.reset_index(drop=True, inplace=True)
    # only include columns that are relevant to Table 1
    project_specs = lab_data[['SAMPID','PROJNAME','SAMPDATE','MATRIX','Table1 Group']]
    project_specs = project_specs[['SAMPID','PROJNAME','MATRIX','Table1 Group']].drop_duplicates()
#     # check duplicates if given ValueError: "Index contains duplicate entries, cannot reshape"
#     display(project_specs[project_specs.duplicated(['SAMPID', 'Table1 Group'])])
    # create table where a check mark is given to each analyte group that is run for each sample
    analysis_checks = project_specs.pivot(index='SAMPID', columns='Table1 Group', values='MATRIX')
    analysis_checks[~analysis_checks.isnull()] = u'\u2713' #reverses isnull to all not null vales and turns them to checks
    
    Table_1 = pd.merge(
        table1_info,
        analysis_checks,
        how = 'left',
        left_on='SAMPID',
        right_on = 'SAMPID',
    )
    # renaming columns for output
    Table_1.rename(
        columns = {
            #'PROJNAME':'Project Name',  
            'SAMPDATE':'Sample Date',
            'SAMPID':'Sample ID', 
        }, 
    inplace = True)
    # add asbestos col if necessary
    if "Asbestos" not in Table_1.columns:
        # add blank asbestos column to metals table
        Table_1["Asbestos"] = np.nan
    # Fill NAs with "- -"
    Table_1 = Table_1.applymap(turn_NaN_to_dash)
    # Merge Table 1 with Sample Order to sort
    Table_1 = pd.merge(
        Table_1,
        sample_order_df,
        how = 'outer',
        left_on='Sample ID',
        right_on='Sample ID',
    )
    # Sort table by Order col, then drop Order col
    Table_1.sort_values(
        by = ['Order'], 
        axis =0,
        inplace = True,
        na_position = 'last'
    ) 
    Table_1.drop(
        ['Order'], 
        axis = 1, 
        inplace = True
    )
    return Table_1


def drop_ND(df, analyte_group, drop_value):
    filtered_df = df[df["Analyte Group"] == analyte_group]
    for analyte in filtered_df["ANALYTE"].unique():
        filtered_by_analyte = filtered_df[
            filtered_df["ANALYTE"] == analyte
        ]  # further filter dataframe by analyte

        all_same = (
            len(set(filtered_by_analyte["FINALVAL"])) == 1
        )  # the length of the set will be 1 if all values are the same, if not, len will be > 1
        if all_same == True:
            index_to_drop = filtered_by_analyte[
                filtered_by_analyte["FINALVAL"] == drop_value
            ].index
            df = df.drop(index_to_drop)
    return df


def drop_ND_by_table(lab_data, tables_for_nd_drop=tables_for_nd_drop):
    full_dropped_analyte_groups = []
    partial_dropped_analyte_groups = []
    for analyte_group in tables_for_nd_drop:
        cleaner_lab_data = drop_ND(lab_data, analyte_group, "ND")
        if not lab_data.equals(cleaner_lab_data):
            if analyte_group in cleaner_lab_data["Analyte Group"].values:
                partial_dropped_analyte_groups.append(analyte_group)
            else:
                full_dropped_analyte_groups.append(analyte_group)
        lab_data = cleaner_lab_data.reset_index(drop=True)
        # if a dropped analyte group is not in lab data anymore, then all of them were dropped
    #print(full_dropped_analyte_groups)
    #print(partial_dropped_analyte_groups)
    return lab_data, partial_dropped_analyte_groups, full_dropped_analyte_groups


def generate_output_tables(lab_data):
    # Divide units into dataframes by analyte group, and pivot on analyte
#    print("g1")
    
    output_tables = {}
    #print("g2")
    #for analyte_group in lab_data["Analyte Group"].unique():
        #print(analyte_group)
    #print("g2 end")
    for analyte_group in lab_data[
        "Analyte Group"
    ].unique():  # creating keys by analyte group
        #print("g3")
        #print(analyte_group)
        output_tables[analyte_group] = pivot_analyte_group_data(
            lab_data[lab_data["Analyte Group"] == analyte_group]
        )  
 #       print(output_tables[analyte_group])
        # creating values to the keys by pivoting the dataframe and using results for only that analyte group
    len(
        output_tables.keys()
    )  # should equal the number of analyte groups represented in lab report
    # output_tables is a DICTIONARY
    #print(len(output_tables.keys()))
    #print("g4")

    if "Soil Metals" in output_tables.keys():
        if "Asbestos" not in output_tables["Soil Metals"].columns:
            # add blank asbestos column to metals table
            output_tables["Soil Metals"]["Asbestos"] = np.nan
    if "Other Inorganic" in output_tables.keys():
        if "Methane" not in output_tables["Other Inorganic"].columns:
            # add blank methane column to other inorganic table
            output_tables["Other Inorganic"]["Methane"] = np.nan    
    # output_tables["Soil Metals"]
    #print("g5")
 
    return output_tables


def split_output_tables(output_tables, soil_tables_list, gw_tables_list, sv_tables):
    soil_tables = {}
    gw_tables = {}
    sv_tables = {}

    for key, value in output_tables.items():
        if key in soil_tables_list:
            soil_tables[key] = value
        if key in gw_tables_list:
            gw_tables[key] = value
        if key in sv_tables_list:
            sv_tables[key] = value

    return soil_tables, gw_tables, sv_tables


def generate_output_tables_stats(output_tables):
    # format results to be summarized (remove <)
    output_tables_as_floats = {}
    for analyte_group, dataframe in output_tables.items():
        output_tables_as_floats[analyte_group] = dataframe.applymap(format_to_floats)

    # Find the ave, min, and max of each analyte per sample
    # create a new dictionary containing the stats of each analyte group, still separated by analyte group as keys
    output_tables_stats = {}
    for key in output_tables_as_floats.keys():
        # case where there are no numerical results in whole table
        if 'mean' in output_tables_as_floats[key].describe().index: #if stats exist
            output_tables_stats[key] = output_tables_as_floats[key].describe().loc[["mean", "min", "max"]].round(3)
        else: #create an empty dataframe with mean, min, max as index
            output_tables_stats[key] = pd.DataFrame(index = ["mean", "min", "max"])            

    return output_tables_stats


def fill_all_non_results(output_tables, lab_data, partial_dropped_analyte_groups):
    # Fill NAs with "NR" in add-on results
    for analyte_group in output_tables.keys():
        for col in output_tables[analyte_group].columns:
            if "WET" in col or "TCLP" in col:
                output_tables[analyte_group][col] = output_tables[analyte_group][
                    col
                ].apply(turn_NaN_to_NR)
    # Fill NAs with "--" in results
    for analyte_group in lab_data["Analyte Group"].unique():
        output_tables[analyte_group] = output_tables[analyte_group].applymap(
            turn_NaN_to_dash
        )
    # add ND columns in specified analyte group
    for analyte_group in partial_dropped_analyte_groups:
        output_tables[analyte_group][f"Other {analyte_group}"] = "ND"


def generate_footnotes_tables(footnotes_df, lab_data):
    # Divide footnotes into dataframes by analyte group
    footnotes_tables = {}
#    print("f1")
 
    for analyte_group in lab_data[
        "Analyte Group"
    ].unique():  # creating keys by analyte group
#        print(analyte_group)
        colName=analyte_group+" Notes"
        if not colName in footnotes_df.columns:
            footnotes_tables[colName]="None"
        footnotes_tables[analyte_group + " Notes"] = footnotes_df[
            analyte_group + " Notes"
        ].to_frame()
#        print("f3")
 
    footnotes_tables["Table 1 List Notes"] = footnotes_df[
        "Table 1 List Notes"
    ].to_frame()
#    print("f4")
    return footnotes_tables


def join_table_components(
    display_criteria_data_soil,
    display_criteria_data_GW,
    display_criteria_data_SV,
    output_tables_stats,
    units_analyte_group,
    soil_tables,
    gw_tables,
    sv_tables,
    Table_1,
    sample_order_df
):
                        ## SOIL TABLES ##
    # joining only units and reg values first: keeps Asb and non-numerical values
    # if chemical is not in references xls, it will get dropped here
    output_tables_crit_stats = {}
    for key in soil_tables.keys():
         output_tables_crit_stats[key] = pd.concat(   
                [
                    display_criteria_data_soil,
                    output_tables_stats[key]
                ],
                axis=0, # along columns
                join='inner', # inner does an intersection instead of a union, outer doesn't work b/c "plan shapes are not aligned"
                sort=False,
                ignore_index=True, # ignoring index prevents concat from also using the index (row names) as an intersection
            ).set_index(pd.Index(list(display_criteria_data_soil.index)+
                                 list(output_tables_stats[key].index))) # must set index to re-add row names
    # Merge: Results with Order column to sort samples
    soil_tables_ordered = {}
    sampleorder = sample_order_df.set_index('Sample ID')
    for key in soil_tables.keys():
        soil_tables_ordered[key] = soil_tables[key].merge(
            sampleorder,
            how = 'inner',
            left_index=True,
            right_index=True,
        )
        # Sort table by Order col, then drop Order col
        soil_tables_ordered[key].sort_values(
            by = ['Order'], 
            axis =0,
            inplace = True,
            na_position = 'first'
        ) 
        soil_tables_ordered[key].drop(
            ['Order'], 
            axis = 1, 
            inplace = True
        )
    # Concatenate: units with crit/stat with results
    # Note: will break on "join = inner" if the analyte is NOT in the regulatory database***
    output_tables_concat_ALL = {}
    for key in soil_tables_ordered.keys():
         output_tables_concat_ALL[key] = pd.concat(   
                [
                    units_analyte_group[key],
                    output_tables_crit_stats[key],
                    soil_tables_ordered[key]
                ],
                axis=0, # along columns
                join='outer', # should work with inner (does an intersection instead of a union) but doesnt work
                sort=False,
                ignore_index=True, # ignoring index prevents concat from also using the index (row names) as an intersection
            ).set_index(pd.Index(list(units_analyte_group[key].index) +
                                 list(output_tables_crit_stats[key].index) + 
                                 list(soil_tables_ordered[key].index))) # must set index to re-add row names
    
                        ## GROUNDWATER TABLES ##
    # Concatenate: stats with references to keep NEG values
    # joining only units and reg values first: keeps Asb and NA's/1
    output_tables_crit_units_GW = {}
    for key in gw_tables.keys():
         output_tables_crit_units_GW[key] = pd.concat(   
                [
                    units_analyte_group[key],
                    display_criteria_data_GW
                ],
                axis=0, # along columns
                join='inner', # inner does an intersection instead of a union, outer doesn't work b/c "plan shapes are not aligned"
                sort=False,
                ignore_index=True, # ignoring index prevents concat from also using the index (row names) as an intersection
            ).set_index(pd.Index(list(units_analyte_group[key].index)+
                                 list(display_criteria_data_GW.index))) # must set index to re-add row names
    # Merge: Results with Order column to sort samples
    gw_tables_ordered = {}
    for key in gw_tables.keys():
        gw_tables_ordered[key] = gw_tables[key].merge(
            sampleorder,
            how = 'inner',
            left_index=True,
            right_index=True,
        )
        # Sort table by Order col, then drop Order col
        gw_tables_ordered[key].sort_values(
            by = ['Order'], 
            axis =0,
            inplace = True,
            na_position = 'first'
        ) 
        gw_tables_ordered[key].drop(
            ['Order'], 
            axis = 1, 
            inplace = True
        )
    # Concatenate: units/crit with results
    # Note: will break on "join = inner" if the analyte is NOT in the regulatory database***
    output_tables_concat_ALL_GW = {}
    for key in gw_tables.keys():
         output_tables_concat_ALL_GW[key] = pd.concat(   
                [
                    output_tables_crit_units_GW[key],
                    gw_tables[key]
                ],
                axis=0, # along columns
                join='outer', # should work with inner (does an intersection instead of a union) but doesnt work
                sort=False,
                ignore_index=True, # ignoring index prevents concat from also using the index (row names) as an intersection
            ).set_index(pd.Index(list(output_tables_crit_units_GW[key].index) + 
                                 list(gw_tables[key].index))) # must set index to re-add row names    

                                ## SOIL VAPOR TABLES ##
    # joining only stats and reg values first: keeps Asb and non-numerical values
    # if chemical is not in references xls, it will get dropped here
    output_tables_crit_stats_sv = {}
    for key in sv_tables.keys():
         output_tables_crit_stats_sv[key] = pd.concat(   
                [
                    display_criteria_data_SV,
                    output_tables_stats[key]
                ],
                axis=0, # along columns
                join='inner', # inner does an intersection instead of a union, outer doesn't work b/c "plan shapes are not aligned"
                sort=False,
                ignore_index=True, # ignoring index prevents concat from also using the index (row names) as an intersection
            ).set_index(pd.Index(list(display_criteria_data_SV.index)+
                                 list(output_tables_stats[key].index))) # must set index to re-add row names
    # Merge: Results with Order column to sort samples
    sv_tables_ordered = {}
    for key in sv_tables.keys():
        sv_tables_ordered[key] = sv_tables[key].merge(
            sampleorder,
            how = 'inner',
            left_index=True,
            right_index=True,
        )
        # Sort table by Order col, then drop Order col
        sv_tables_ordered[key].sort_values(
            by = ['Order'], 
            axis =0,
            inplace = True,
            na_position = 'first'
        ) 
        sv_tables_ordered[key].drop(
            ['Order'], 
            axis = 1, 
            inplace = True
        )
    # Concatenate: units with crit/stat with results
    # Note: will break on "join = inner" if the analyte is NOT in the regulatory database***
    output_tables_concat_ALL_SV = {}
    for key in sv_tables_ordered.keys():
         output_tables_concat_ALL_SV[key] = pd.concat(   
                [
                    units_analyte_group[key],
                    output_tables_crit_stats_sv[key],
                    sv_tables_ordered[key]
                ],
                axis=0, # along columns
                join='outer', # should work with inner (does an intersection instead of a union) but doesnt work
                sort=False,
                ignore_index=True, # ignoring index prevents concat from also using the index (row names) as an intersection
            ).set_index(pd.Index(list(units_analyte_group[key].index) +
                                 list(output_tables_crit_stats_sv[key].index) + 
                                 list(sv_tables_ordered[key].index))) # must set index to re-add row names

    # add soil, groundwater, and soil vapor tables together in final_output_tables
    final_output_tables = {**output_tables_concat_ALL,**output_tables_concat_ALL_GW,**output_tables_concat_ALL_SV}
    
    # Add Table 1 to Dictionary
    final_output_tables['Table 1 List'] = Table_1
    # Reorganize tables
    # Rename Analytes to Display names
    for key in final_output_tables.keys():
        final_output_tables[key].rename(columns=analyte_name_to_display_name, inplace = True)
#    print("Key:"+final_output_tables[key])
        
    # Reorder columns
    for key in final_output_tables.keys():
    #    display_name_order = intersection(final_output_tables[key].columns,display_names)
        final_output_tables[key] = reorder_columns(final_output_tables[key],output_table_col_order[key])
    
        # turn blank cells (Nan's) to "NA"
        final_output_tables[key].fillna('NA', inplace=True)
#    print("Reorder Key:"+final_output_tables[key])
    return final_output_tables


def format_results_with_rls(row, criteria_to_compare):
    # get smallest reg for comparison
    analyte = row["ANALYTE"]
    if analyte in criteria_to_compare.columns:
        criteria_data = criteria_to_compare[analyte]
        criteria_data_no_strings = [x for x in criteria_data if not isinstance(x, str)]
    else:
        criteria_data_no_strings = []
    if len(criteria_data_no_strings) != 0:
        smallest_reg = min(criteria_data_no_strings)
    else:
        smallest_reg = None
    # deal with special asbestos ND case
    if row["ANALYTE"] == "Asbestos" and row["FINALVAL"] == "ND":
        return row["FINALVAL"]
    # deal with ND, and "RL too high"
    if row["FINALVAL"] == "ND":
        if smallest_reg and row["PQL"] > smallest_reg:
            # rounding to 6 decimal places prevents trailing 0s
            return f'<{round(row["PQL"],6)}*'
        else:
            return f'<{round(row["PQL"],6)}'
    if row["FINALVAL"] == "neg" or row["FINALVAL"] == "NEG":
        return "NEG"
    if row["FINALVAL"] == "pos" or row["FINALVAL"] == "POS":
        return "POS"
    if "<" in str(row["FINALVAL"]):
        return row["FINALVAL"]
    if "@" in str(row["FINALVAL"]):
        return row["FINALVAL"]
    else:
        return float(row["FINALVAL"])


def generate_formatted_lab_report(
    lab_data,
    regulatory_criteria_references_database,
    reg_data_preferences,
    footnotes_df,
    sample_order_df,
):
#    print("No of Records : " + str(len(lab_data)) + " processed.")
    crit_list = reg_data_preferences
    comparison_crit_list = reg_data_preferences
    #####

    # Split criteria into soil and gw lists for display
    crit_list_soil = crit_list.copy()
    crit_list_gw = []
    crit_list_sv = []
    for reference in gw_criteria:
        if reference in crit_list:
            crit_list_soil.remove(reference)
            crit_list_gw.append(reference)
    for reference in sv_criteria:
        if reference in crit_list:
            crit_list_soil.remove(reference)
            crit_list_sv.append(reference
                           )

    # Split comparison criteria into soil, gw, and sv lists
    compare_list_soil = intersection(crit_list_soil, comparison_crit_list)
    compare_list_gw = intersection(crit_list_gw, comparison_crit_list)
    compare_list_sv = intersection(crit_list_sv, comparison_crit_list)
    compare_list_nonsoil = intersection(crit_list_gw + crit_list_sv, comparison_crit_list)

    # Comparison HEALTH criteria 
    compare_health_list_soil = intersection(compare_list_soil, health_crit_list)
    compare_health_list_gw = intersection(compare_list_gw, health_crit_list)
    compare_health_list_sv = intersection(compare_list_sv, health_crit_list)

    # turn crit_list into a dataframe
    criteria_data_specified = regulatory_criteria_references_database[
        ["Reference"] + crit_list
    ]
    criteria_data_specified_transposed = criteria_data_specified.transpose()
    criteria_data_specified_transposed.columns = criteria_data_specified_transposed.loc[
        "Reference"
    ]  # makes the column names be the 'References' instead of the index which happens after transposing
    criteria_data_specified_transposed.drop(
        "Reference", axis=0, inplace=True
    )  # gets rid of the first row which was the References, now no longer needed

    criteria_to_compare_all = criteria_data_specified_transposed.loc[
        comparison_crit_list, :
    ]

    # Criteria data to display on tables
    display_criteria_data_GW = criteria_data_specified_transposed.loc[crit_list_gw, :]
    display_criteria_data_soil = criteria_data_specified_transposed.loc[crit_list_soil,:]
    display_criteria_data_SV = criteria_data_specified_transposed.loc[crit_list_sv,:]

    # Criteria data to compare results
    comparison_data_GW = criteria_data_specified_transposed.loc[compare_list_gw,:]
    comparison_data_soil = criteria_data_specified_transposed.loc[compare_list_soil,:]
    comparison_data_SV = criteria_data_specified_transposed.loc[compare_list_sv,:]
    comparison_data_nonsoil = criteria_data_specified_transposed.loc[compare_list_nonsoil,:]

    # set up the HEALTH criteria to compare results against 
    # AND to display for soil, groundwater, and soil vapor

    # SOIL
    #print("A")
    display_soil_criteria_index = {}
    for item in crit_list_soil:
        display_soil_criteria_index[item] = crit_list_soil.index(item) + 4

    health_min_range_rows = []
    for key, value in display_soil_criteria_index.items():
        if key in compare_health_list_soil:
            health_min_range_rows.append(value)
    #print("A")
    
    # GROUNDWATER
    display_gw_criteria_index = {}
    for item in crit_list_gw:
        display_gw_criteria_index[item] = crit_list_gw.index(item) + 4

    gw_health_min_range_rows = []
    for key, value in display_gw_criteria_index.items():
        if key in compare_health_list_gw:
            gw_health_min_range_rows.append(value)

    # SOIL VAPOR
    display_sv_criteria_index = {}
    for item in crit_list_sv:
        display_sv_criteria_index[item] = crit_list_sv.index(item)+4
        
    sv_health_min_range_rows = []
    for key,value in display_sv_criteria_index.items():
        if key in compare_health_list_sv:
            sv_health_min_range_rows.append(value)



    # build a dictionary with lists of regulatory criteria by category
    chosen_regulatory_criteria = {
        category: intersection(regulatory_category_dict[category], crit_list)
        for category in regulatory_category_dict.keys()
    }

    # Apply processing to lab_data according to the following logic based on what types of data files are input
    lab_data_to_sort = drop_unwanted_analytes(lab_data)
    soil_df, addon_df, GW_df, SV_df, non_soil_df = sort_non_soil_data(lab_data_to_sort)
    len_soil_samples = len(soil_df['SAMPID'].unique())
    len_gw_samples = len(GW_df['SAMPID'].unique())
    len_sv_samples = len(SV_df['SAMPID'].unique())
    #print("B")

    if soil_df.empty==False:
        # process soil lab data
        #print("C")
        processed_lab_data = preprocess_soil_lab_data(soil_df, comparison_data_soil)
        # For Soil and GW/SV Data
        if non_soil_df.empty==False:
            # process GW/SV lab data and combine with soil lab data
            processed_nonsoil_data = preprocess_nonsoil_data(non_soil_df, comparison_data_nonsoil)
            combined_lab_data_nonsoil = combine_data(processed_lab_data, processed_nonsoil_data)
            lab_data_pre = combined_lab_data_nonsoil
        else:
            lab_data_pre = processed_lab_data
        # For Soil and Add-On Data (optional: GW/SV Data)
        if addon_df.empty==False:
            # process add-on lab data and combine with soil &/or GW/SV lab data
            processed_lab_addon_data = preprocess_lab_addon_data(addon_df,comparison_data_soil)
            combined_lab_data_addon = combine_data(lab_data_pre, processed_lab_addon_data)
            lab_data = combined_lab_data_addon
        else:
            lab_data = lab_data_pre
    # IF soil data does not exist, check if GW/SV data exists and process
    else:
        #print("D")
        if non_soil_df.empty==False:
            # For GW/SV and Add-on data
            if addon_df.empty==False:
                processed_nonsoil_data = preprocess_nonsoil_data(non_soil_df, comparison_data_nonsoil)
                processed_lab_addon_data = preprocess_lab_addon_data(addon_df, comparison_data_soil)
                combined_addon_nonsoil = combine_data(processed_nonsoil_data, processed_lab_addon_data)
                lab_data = combined_addon_nonsoil
            # For Only GW/SV Data
            else:
                processed_nonsoil_data = preprocess_nonsoil_data(non_soil_df, comparison_data_nonsoil)
                lab_data = processed_nonsoil_data
        # For Only Add-on data
        else:
            processed_lab_addon_data = preprocess_lab_addon_data(addon_df)
            lab_data = processed_lab_addon_data
        
    lab_data.reset_index(inplace=True,drop=True)

    # Generate Summary Table
    Table_1 = generate_table_1(lab_data, sample_order_df)

    (
        lab_data,
        partial_dropped_analyte_groups,
        full_dropped_analyte_groups,
    ) = drop_ND_by_table(lab_data)

    output_tables = generate_output_tables(lab_data)
    soil_tables, gw_tables, sv_tables = split_output_tables(
        output_tables, soil_tables_list, gw_tables_list, sv_tables_list
    )
    # Summarize Data
    output_tables_stats = generate_output_tables_stats(output_tables)
    fill_all_non_results(output_tables, lab_data, partial_dropped_analyte_groups)

    # Fill NAs with "--" in results
    for analyte_group in lab_data[
        "Analyte Group"
    ].unique():  # creating keys by analyte group
        output_tables[analyte_group] = output_tables[analyte_group].applymap(
            turn_NaN_to_dash
        )

    # add ND columns in specified analyte group
    for analyte_group in partial_dropped_analyte_groups:
        output_tables[analyte_group][f"Other {analyte_group}"] = "ND"

    # (this must be applied after ND columns/groups are dropped, so only the relevant footnotes are created as dfs)
    #print("R1")
    
    footnotes_tables = generate_footnotes_tables(footnotes_df, lab_data)

    #print("R2")
    
    # Create a dictionary for UNITS
    units_analyte_group = {}
    #print("R3")
    
    # creating keys by analyte group
    for analyte_group in lab_data["Analyte Group"].unique():
        units_analyte_group[analyte_group] = transpose_analyte_group_units(
            lab_data[lab_data["Analyte Group"] == analyte_group]
        )

    #print("R4")
    print(display_criteria_data_soil)
    
    print(soil_tables)
    # Join together table components
    final_output_tables = join_table_components(
        display_criteria_data_soil,
        display_criteria_data_GW,
        display_criteria_data_SV,
        output_tables_stats,
        units_analyte_group,
        soil_tables,
        gw_tables,
        sv_tables,
        Table_1,
        sample_order_df
    )
    print("final output tables")
    print(final_output_tables)
    #print("R5")
    
 #   print(final_output_tables)
    return (
        final_output_tables,
        output_tables,
        chosen_regulatory_criteria,
        footnotes_tables,
        health_min_range_rows,
        gw_health_min_range_rows,
        sv_health_min_range_rows,
        len_soil_samples,
        len_gw_samples,
        len_sv_samples,
    )



def format_table1(sheet, final_output_tables, len_soil_samples, len_gw_samples, len_sv_samples):
    ## define sheet specific parameters
    table1_len = (
        len(final_output_tables["Table 1 List"].columns) + 3
    )  # add 3 for the columns that will be inserted
    table1_len_letter = get_column_letter(table1_len)
    header_row = 1
    header_range = f"A1:{table1_len_letter}1"
    sample_len = len(final_output_tables["Table 1 List"])
    sample_range = f"A2:{table1_len_letter}{sample_len+1}"
    T1_range = f"A1:{table1_len_letter}{sample_len+1}"
    checkmark_range = f'F2:{table1_len_letter}{sample_len+1}'
    color_col_number = (
        len(final_output_tables["Table 1 List"].columns) + 4
    )  # will add a column at end of table
    color_col = get_column_letter(color_col_number)
    start_row = 2
    stop_row = sample_len + 1
    # Soil samples header info
    soil_header_row = 2
    soil_header_loc = f'B{soil_header_row}'
    soil_header_range = f'{soil_header_loc}:{table1_len_letter}{soil_header_row}'
    # Groundwater samples header info
    if len_soil_samples > 0:
        gw_header_row = soil_header_row + len_soil_samples + 1
    else:
        gw_header_row = soil_header_row
    gw_header_loc = f'B{gw_header_row}'
    gw_header_range = f'{gw_header_loc}:{table1_len_letter}{gw_header_row}'
    # Soil Vapor samples header info
    if len_gw_samples > 0:
        sv_header_row = gw_header_row + len_gw_samples + 1
    else:
        if len_soil_samples > 0:
            sv_header_row = soil_header_row + len_soil_samples + 1
        else:
            sv_header_row = soil_header_row
    sv_header_loc = f'B{sv_header_row}'
    sv_header_range = f'{sv_header_loc}:{table1_len_letter}{sv_header_row}'

    # insert a col left of table 1 to anchor the tables to, using a duplicate Sample ID column
    sheet.insert_cols(1)
    for cell in sheet["B:B"]:
        sheet.cell(row=cell.row, column=1, value=cell.value)
    sheet["A1"].value = "Anchor"

    ## add columns for user input info
    # composte ID col
    sheet.insert_cols(2)
    # add 'composite' cell to heading
    sheet["B1"].value = "Composite Sample ID"
    sheet["B1"].alignment = Alignment(horizontal="center", vertical="bottom")
    # depth col
    sheet.insert_cols(4)
    # add 'depth' cell to heading
    sheet["D1"].value = "Depth (bgs)"
    sheet["D1"].alignment = Alignment(horizontal="center", vertical="bottom")

    ## STYLING
    # wrap header column
    wrap_headers(sheet, header_row, table1_len, 8)
    # set the height of the header row
    sheet.row_dimensions[header_row].height = 30
    # specify heading widths more specifically
    sheet.column_dimensions["B"].width = 10
    sheet.column_dimensions["C"].width = 15
    sheet.column_dimensions["D"].width = 8
    sheet.column_dimensions["E"].width = 12
    #         # set column width for analyte groups
    #         for col_letter in enumerate(column_widths):
    #             worksheet.column_dimensions[col_letter].width = 8

    # style header row
    set_grey_fill(sheet, header_range)
    # style all cells in sheet
    center_cell(sheet, T1_range)
    set_border(sheet, T1_range)
    set_check_font(sheet, checkmark_range)

    # create column to indicate background color with alternating numbers
    alt_numbers_col(color_col_number, start_row, stop_row, sheet)
    # style all sample rows with alternating background color
    alt_row_color(sample_range, f"{color_col}2", sheet)
    # hide background color column
    sheet.column_dimensions[f"{color_col}"].hidden = True
    # hide anchor colomn
    sheet.column_dimensions["A"].hidden = True

    if len_soil_samples > 0:
        sheet.insert_rows(soil_header_row)
        sheet[soil_header_loc].value = 'Soil Samples'
        set_table1_header_format(sheet, soil_header_range)
        sheet[soil_header_loc].border = Border(left=thin_black_border)
        sheet[f'{table1_len_letter}{soil_header_row}'].border = Border(right=thin_black_border)

    if len_gw_samples > 0:
        sheet.insert_rows(gw_header_row)
        sheet[gw_header_loc].value = 'Groundwater Samples'
        set_table1_header_format(sheet, gw_header_range)
        sheet[gw_header_loc].border = Border(left=thin_black_border)
        sheet[f'{table1_len_letter}{gw_header_row}'].border = Border(right=thin_black_border)

    if len_sv_samples > 0:
        sheet.insert_rows(sv_header_row)
        sheet[sv_header_loc].value = 'Soil Vapor Samples'
        set_table1_header_format(sheet, sv_header_range)
        sheet[sv_header_loc].border = Border(left=thin_black_border)
        sheet[f'{table1_len_letter}{sv_header_row}'].border = Border(right=thin_black_border)

def format_soiltables(
    sheet, name, output_tables, final_output_tables, chosen_regulatory_criteria, health_min_range_rows
):
 #   print(final_output_tables)
#    print(sheet)
#    print(name)
#    print(name.find('FAS'))
#    print(name.find('PFAS'))
    # delete random blank row
    sheet.delete_rows(2)
 #   print(output_tables.items(name))
 #   if (name=="SVOCs"):
 #       print ("E1")
    ### DEFINE SHEET SPECIFIC PARAMETERS ###
    link_to=sheet
    intro_cols = 4 + 1  # add an extra col to account for anchor col
    df_col_len = len(final_output_tables[name].columns) + intro_cols
    col_analyte=len(output_tables[name].columns)
    length_samples = len(output_tables[name])
    startDataResult_col=6
    first_data_col_letter = "F"
    color_col_number = len(final_output_tables[name].columns) + intro_cols + 1
#    if (name=="SVOCs"):
#        print ("E2")

    if name.find('VOC')!=-1: #+1 to include Other
        df_col_len = df_col_len +1
        color_col_number=color_col_number+1
    
    if name.find('Pesticides')!=-1:
        df_col_len = df_col_len + 1
        color_col_number=color_col_number+1
        
    if name.find('PFA')!=-1:
        df_col_len = df_col_len + 1    
        color_col_number=color_col_number+1
    
    color_col = get_column_letter(color_col_number)
    df_col_len_letter = get_column_letter(df_col_len)    
    ordering_col_number = color_col_number + 1
    ordering_col = get_column_letter(ordering_col_number)
#    if (name=="SVOCs"):
 #       print ("E3")
    
#    print("df_col_len:"+ str(df_col_len))
#    print("df_col_len_letter:" + str(df_col_len_letter))
#    print("length_samples:"+ str(length_samples))
#    print("column of analyte :" + str(col_analyte))
    
#    print(output_tables[name])
#    print("color_col_number:" + str(color_col_number))
#    print("color_col:" + str(color_col))
#    print("ordering_col_number:" + str(ordering_col_number))
#    print("ordering_col:" + str(ordering_col))

    if name in extra_header_list:
        header_len = 3
        first_analyte_header_loc = "F$2"
        first_analyte_header_row=2
        health_min_range_rows = [x+1 for x in health_min_range_rows]
 #       print("health_min_range_rows:" + str(health_min_range_rows))
    else:
        header_len = 2  # analyte name row and units row
        first_analyte_header_loc = "F$1"
        first_analyte_header_row=1
    health_min_range_list = [f"F${num}" for num in health_min_range_rows]
    health_min_range_str = ",".join(health_min_range_list)
#    print("health_min_range_list:" + str(health_min_range_list))
#    print("health_min_range_str:" + str(health_min_range_str))
#    if (name=="SVOCs"):
#        print ("E4")


    # Criteria Info
    ## Risk and Regulatory Criteria
    if len(chosen_regulatory_criteria["Risk Regulatory and Reference Criteria"]) > 0:
        risk_and_reg_crit_len = (
            len(chosen_regulatory_criteria["Risk Regulatory and Reference Criteria"])
            + 1
        )
        risk_reg_pre_groups = header_len  # all of the other regulatory groups that come before each reg. group
        risk_and_reg_crit_header_loc = f"B{risk_reg_pre_groups + 1}"
        risk_and_reg_start_row = risk_reg_pre_groups + 2
        risk_and_reg_crit_first_loc = f"B{risk_and_reg_start_row}"
        risk_and_reg_stop_row = risk_reg_pre_groups + risk_and_reg_crit_len
        risk_and_reg_crit_last_loc = f"{df_col_len_letter}{risk_and_reg_stop_row}"
        risk_range = f"{risk_and_reg_crit_first_loc}:{risk_and_reg_crit_last_loc}"
        risk_color_loc = f"{color_col}{risk_reg_pre_groups + 2}"
        health_min_range = health_min_range_str
    else:
        risk_and_reg_crit_len = 0
#    if (name=="SVOCs"):
#        print ("E5")

    ## Hazardous Waste Criteria
    if len(chosen_regulatory_criteria["Hazardous Waste Criteria"]) > 0:
        haz_waste_crit_len = (
            len(chosen_regulatory_criteria["Hazardous Waste Criteria"]) + 1
        )
        haz_crit_pre_groups = header_len + risk_and_reg_crit_len
        haz_crit_header_loc = f"B{haz_crit_pre_groups + 1}"
        haz_start_row = haz_crit_pre_groups + 2
        haz_crit_first_loc = f"B{haz_start_row}"
        haz_stop_row = haz_crit_pre_groups + haz_waste_crit_len
        haz_crit_last_loc = f"{df_col_len_letter}{haz_stop_row}"
        haz_range = f"{haz_crit_first_loc}:{haz_crit_last_loc}"
        haz_color_loc = f"{color_col}{haz_crit_pre_groups + 2}"
        TTLC_loc = f"{first_data_col_letter}${haz_crit_pre_groups + 2}"
        STLCx10_loc = f"{first_data_col_letter}${haz_crit_pre_groups + 3}"
        STLC_loc = f"{first_data_col_letter}${haz_crit_pre_groups + 4}"
        TCLPx20_loc = f"{first_data_col_letter}${haz_crit_pre_groups + 5}"
        TCLP_loc = f"{first_data_col_letter}${haz_crit_pre_groups + 6}"
    else:
        haz_waste_crit_len = 0
    ## Recycling Acceptance Criteria
    if len(chosen_regulatory_criteria["Recycling Facility Acceptance Criteria"]) > 0:
        recyc_crit_len = (
            len(chosen_regulatory_criteria["Recycling Facility Acceptance Criteria"])
            + 1
        )
        recyc_crit_pre_groups = header_len + risk_and_reg_crit_len + haz_waste_crit_len
        recyc_crit_header_loc = f"B{recyc_crit_pre_groups + 1}"
        recyc_start_row = recyc_crit_pre_groups + 2
        recyc_crit_first_loc = f"B{recyc_start_row}"
        recyc_stop_row = recyc_crit_pre_groups + recyc_crit_len
        recyc_ctit_last_loc = f"{df_col_len_letter}{recyc_stop_row}"
        recycling_range = f"{recyc_crit_first_loc}:{recyc_ctit_last_loc}"
        recycling_color_loc = f"{color_col}{recyc_crit_pre_groups + 2}"
    else:
        recyc_crit_len = 0
#    if (name=="SVOCs"):
#        print ("E6")

    ## Potential Background Concentraction Criteria
    if (
        len(chosen_regulatory_criteria["Range of Potential Background Concentration"])
        > 0
    ):
        background_crit_len = (
            len(
                chosen_regulatory_criteria[
                    "Range of Potential Background Concentration"
                ]
            )
            + 1
        )
        background_crit_pre_groups = (
            header_len + risk_and_reg_crit_len + haz_waste_crit_len + recyc_crit_len
        )
        background_crit_header_loc = f"B{background_crit_pre_groups + 1}"
        background_start_row = background_crit_pre_groups + 2
        background_crit_first_loc = f"B{background_start_row}"
        background_stop_row = background_crit_pre_groups + background_crit_len
        background_crit_last_loc = f"{df_col_len_letter}{background_stop_row}"
        background_range = f"{background_crit_first_loc}:{background_crit_last_loc}"
        background_color_loc = f"{color_col}{background_crit_pre_groups + 2}"
    else:
        background_crit_len = 0

    # data info
    reg_criteria_len = (
        risk_and_reg_crit_len
        + haz_waste_crit_len
        + recyc_crit_len
        + background_crit_len
    )
#    if (name=="SVOCs"):
#        print ("E7")
    # stats info
    stats_pregroup = header_len + reg_criteria_len
    stats_heading_loc = f'B{stats_pregroup + 1}'
    stats_len = 4
    stats_start_row = stats_pregroup + 2
    stats_stop_row = stats_pregroup + 4
    stats_range = f"B{stats_start_row}:{df_col_len_letter}{stats_stop_row}"
    stats_color_loc = f"{color_col}{stats_pregroup+2}"
    # results info
    result_header_row = header_len + reg_criteria_len + stats_len + 1
    results_start_row = result_header_row + 1
    first_result_loc = f"{first_data_col_letter}{results_start_row}"
#    print("first_result_loc:" + str(first_result_loc))
    results_stop_row = result_header_row + length_samples
    last_result_loc = f"{df_col_len_letter}{results_stop_row}"
    anchor_row = f"A{result_header_row}"
    results_info_1 = f"B{result_header_row}"
    results_info_2 = f"C{result_header_row}"
    results_info_3 = f"D{result_header_row}"
    results_info_4 = f"E{result_header_row}"
    results_info_range = f"B{result_header_row}:E{results_stop_row}"
    results_plusT1_range = f"B{results_start_row}:{last_result_loc}"
    results_range = f"{first_result_loc}:{last_result_loc}"
#   print("results_range")
#    print(results_range)
    results_color_loc = f"{color_col}{results_start_row}"
    ### END OF PARAMETERS ###
#    if (name=="SVOCs"):
#        print ("E8")

    ### Add rows + columns before formatting ###
    # insert a col left of table to anchor the tables to, using a duplicate Sample ID column
    sheet.insert_cols(1)
    for cell in sheet[f"B:B"]:
        sheet.cell(row=cell.row, column=1, value=cell.value)

    # add blank columns to the left of data (3 columns)
    sheet.insert_cols(3, 3)
    # center all values in cells
    center_cell(sheet, f"{first_data_col_letter}1:{last_result_loc}")

    # for tables with extra headings, insert row on top
    if name in extra_header_list:
        sheet.insert_rows(1)

    ### FORMAT TABLES ###

    # CRITERIA HEADINGS
#    if (name=="SVOCs"):
#        print ("E9")

    # Risk Regulatory Criteria Heading
    if len(chosen_regulatory_criteria["Risk Regulatory and Reference Criteria"]) > 0:
        sheet.insert_rows(header_len + 1)  # insert row (before existing row 3)
        sheet[risk_and_reg_crit_header_loc] = "Risk Regulatory and Reference Criteria"
        set_header_format(sheet, f'{risk_and_reg_crit_header_loc}:{df_col_len_letter}{risk_reg_pre_groups + 1}')
        sheet[risk_and_reg_crit_header_loc].border = Border(left=thin_black_border)
        sheet[f'{df_col_len_letter}{risk_reg_pre_groups + 1}'].border = Border(right=thin_black_border)
        set_border(sheet, risk_range)

        alt_numbers_col(color_col_number, risk_and_reg_start_row, risk_and_reg_stop_row, sheet)
        alt_row_color(risk_range, risk_color_loc, sheet)
        # merge criteria rows
        len_list = createList(risk_and_reg_crit_len - 1)
        merge_list = []
        for number in len_list:
            row = (
                f"B{number+ risk_reg_pre_groups + 1}:E{number+ risk_reg_pre_groups + 1}"
            )
            merge_list.append(row)
        for location in merge_list:
            sheet.merge_cells(location)
            center_right_cell(sheet, location)

#    if (name=="SVOCs"):
#        print ("E10")
    # Haz Waste Criteria Heading
    if len(chosen_regulatory_criteria["Hazardous Waste Criteria"]) > 0:
        sheet.insert_rows(header_len + risk_and_reg_crit_len + 1)
        sheet[haz_crit_header_loc] = "Hazardous Waste Criteria"
        set_header_format(sheet, f'{haz_crit_header_loc}:{df_col_len_letter}{haz_crit_pre_groups + 1}')
        sheet[haz_crit_header_loc].border = Border(left=thin_black_border)
        sheet[f'{df_col_len_letter}{haz_crit_pre_groups + 1}'].border = Border(right=thin_black_border)
        set_border(sheet, haz_range)
        alt_numbers_col(color_col_number, haz_start_row, haz_stop_row, sheet)
        alt_row_color(haz_range, haz_color_loc, sheet)
        # merge criteria rows
        len_list = createList(haz_waste_crit_len - 1)
        merge_list = []
        for number in len_list:
            row = (
                f"B{number+ haz_crit_pre_groups + 1}:E{number+ haz_crit_pre_groups + 1}"
            )
            merge_list.append(row)
        for location in merge_list:
            sheet.merge_cells(location)
            center_right_cell(sheet, location)

#    if (name=="SVOCs"):
#        print ("E11")
    # Recycling Criteria Heading
    if len(chosen_regulatory_criteria["Recycling Facility Acceptance Criteria"]) > 0:
        sheet.insert_rows(header_len + risk_and_reg_crit_len + haz_waste_crit_len + 1)
        sheet[recyc_crit_header_loc] = "Recycling Facility Acceptance Criteria"
        set_header_format(sheet, f'{recyc_crit_header_loc}:{df_col_len_letter}{recyc_crit_pre_groups + 1}')
        sheet[recyc_crit_header_loc].border = Border(left=thin_black_border)
        sheet[f'{df_col_len_letter}{recyc_crit_pre_groups + 1}'].border = Border(right=thin_black_border)
        set_border(sheet, recycling_range)
        alt_numbers_col(color_col_number, recyc_start_row, recyc_stop_row, sheet)
        alt_row_color(recycling_range, recycling_color_loc, sheet)
        # merge criteria rows
        len_list = createList(recyc_crit_len - 1)
        merge_list = []
        for number in len_list:
            row = f"B{number+ recyc_crit_pre_groups + 1}:E{number+ recyc_crit_pre_groups + 1}"
            merge_list.append(row)
        for location in merge_list:
            sheet.merge_cells(location)
            center_right_cell(sheet, location)

#    if (name=="SVOCs"):
#        print ("E12")
    # Background Criteria Heading
    if (
        len(chosen_regulatory_criteria["Range of Potential Background Concentration"])
        > 0
    ):
        sheet.insert_rows(
            header_len + risk_and_reg_crit_len + haz_waste_crit_len + recyc_crit_len + 1
        )
        sheet[
            background_crit_header_loc
        ] = "Range of Potential Background Concentration"
        set_header_format(sheet, f'{background_crit_header_loc}:{df_col_len_letter}{background_crit_pre_groups + 1}')
        sheet[background_crit_header_loc].border = Border(left=thin_black_border)
        sheet[f'{df_col_len_letter}{background_crit_pre_groups + 1}'].border = Border(right=thin_black_border)
        set_border(sheet, background_range)
        alt_numbers_col(color_col_number, background_start_row, background_stop_row, sheet)
        alt_row_color(background_range, background_color_loc, sheet)
        # merge criteria rows
        len_list = createList(background_crit_len - 1)
        merge_list = []
        for number in len_list:
            row = f"B{number+ background_crit_pre_groups + 1}:E{number+ background_crit_pre_groups + 1}"
            merge_list.append(row)
        for location in merge_list:
            sheet.merge_cells(location)
            center_right_cell(sheet, location)

#    if (name=="SVOCs"):
#        print ("E13")
    # Stats Heading Row
    sheet.insert_rows(stats_pregroup+1)
    sheet[stats_heading_loc] = "Statistical Evaluation"
    set_header_format(sheet, f'{stats_heading_loc}:{df_col_len_letter}{stats_pregroup + 1}')
    sheet[stats_heading_loc].border = Border(left=thin_black_border)
    sheet[f'{df_col_len_letter}{stats_pregroup + 1}'].border = Border(right=thin_black_border)
    set_border(sheet, stats_range)
    alt_numbers_col(color_col_number, stats_start_row, stats_stop_row, sheet)
    alt_row_color(stats_range, stats_color_loc, sheet)
    # merge stats rows
    merge_list = []
    len_list = createList(3)
    for number in len_list:
        row = f"B{number+ header_len+reg_criteria_len + 1}:E{number+ header_len+reg_criteria_len + 1}"
        merge_list.append(row)
    for location in merge_list:
        sheet.merge_cells(location)
        center_right_cell(sheet, location)

    # space column width according to largest cell
    set_col_width(sheet)

#    if (name=="SVOCs"):
#        print ("E14")

    # merge extra header row on certain tables
    if name in extra_header_list:
        sheet.merge_cells(f"{first_data_col_letter}1:{df_col_len_letter}1")
        sheet[f"{first_data_col_letter}1"].value = analytegroup_table_headings[name]
        sheet[f"{first_data_col_letter}1"].alignment = Alignment(horizontal="center")

    # merge top left cell
    sheet.merge_cells(f"B1:E{header_len}")
    # styling header rows
    units_analytes_cells = f"B1:{df_col_len_letter}{header_len}"
    # add 'units' cell to heading
    sheet["B1"].value = "Units"
    sheet["B1"].alignment = Alignment(horizontal="right", vertical="bottom")
    # format 'units' merged cell background
    grey_fill = PatternFill(end_color="d0cece", start_color="d0cece", fill_type="solid")
    sheet["B1"].fill = grey_fill
    # style analyte and unit rows
    set_grey_fill(sheet, units_analytes_cells)
    set_border(sheet,units_analytes_cells)

    # Results Heading Row
    sheet.insert_rows(result_header_row)
    set_grey_fill(sheet, f'B{result_header_row}:{df_col_len_letter}{result_header_row}')
    set_border(sheet,f'B{result_header_row}:E{result_header_row}')
    sheet[f'{df_col_len_letter}{result_header_row}'].border = Border(right=thin_black_border)

    # fill alternating background color for results
    alt_numbers_col(color_col_number, results_start_row, results_stop_row, sheet)
    alt_row_color(results_plusT1_range, results_color_loc, sheet)
    set_border(sheet,results_plusT1_range)
    # hide background color column
    sheet.column_dimensions[f"{color_col}"].hidden = True
    # delete unnecessary cells in anchor col
    sheet[anchor_row].value = "Anchor"
    for row in sheet[f"A2:A{stats_stop_row}"]:
        for cell in row:
            cell.value = None
    # hide anchor colomn
    sheet.column_dimensions["A"].hidden = True

    # Link Info from Table 1
    # add proj info to all results rows / Link Results Info from Table 1
    link_from = "Table 1 List"
    link_to = sheet
    len_list = createList(length_samples)
#    print("Len List")
#    print(len_list)
    link_list = []

    # link table 1 info HEADERS
    link_to_comp_ID = link_to[results_info_1]
    link_to_comp_ID.value = f"='{link_from}'!B1"
    link_to_comp_ID.alignment = Alignment(
        wrapText=True, horizontal="center", vertical="center"
    )

    link_to_samp_ID = link_to[results_info_2]
    link_to_samp_ID.value = f"='{link_from}'!C1"
    link_to_samp_ID.alignment = Alignment(
        wrapText=True, horizontal="center", vertical="center"
    )

    link_to_date = link_to[results_info_3]
    link_to_date.value = f"='{link_from}'!E1"
    link_to_date.alignment = Alignment(
        wrapText=True, horizontal="center", vertical="center"
    )

    link_to_depth = link_to[results_info_4]
    link_to_depth.value = f"='{link_from}'!D1"
    link_to_depth.alignment = Alignment(
        wrapText=True, horizontal="center", vertical="center"
    )

    # link table 1 info by row, using v-lookup
    # important to use v-lookup incase the order changes
    sampleID_col_Letter="C"
    start_analyte_col=intro_cols+1
    end_analyte_col=df_col_len
    if name.find('VOC')!=-1: #+1 to include Other
        col_letter=get_column_letter(end_analyte_col)
        pre_col_letter=get_column_letter(end_analyte_col-1)
        link_to_Other=link_to[f"{col_letter}1"]
        link_to_Other.value="Other " + name
        link_to_unit=link_to[f"{col_letter}2"]
        link_to_unit.value=link_to[f"{pre_col_letter}2"].value
        
    if name.find('Pesticides')!=-1:
        col_letter=get_column_letter(end_analyte_col)
        pre_col_letter=get_column_letter(end_analyte_col-1)
        link_to_Other=link_to[f"{col_letter}1"]
        link_to_Other.value="Other " + name
        link_to_unit=link_to[f"{col_letter}2"]
        link_to_unit.value=link_to[f"{pre_col_letter}2"].value
        
        
    if name.find("PFAS")!=-1:
        col_letter=get_column_letter(end_analyte_col)
        pre_col_letter=get_column_letter(end_analyte_col-1)
        link_to_Other=link_to[f"{col_letter}1"]
        link_to_Other.value="Other " + name
        link_to_unit=link_to[f"{col_letter}2"]
        link_to_unit.value=link_to[f"{pre_col_letter}2"].value
 
    data_list=output_tables[name]
    analyte_list=list(data_list)
#    samplelist=[]
    y=analyte_list[0]
    row = 1 + result_header_row
    initial_data_row=row
    column_width=0
#    if (name=="SVOCs"):
#        print ("E15")

#    print(analyte_list)
#    print(data_list[analyte_list[0]].items())
#    print(len(data_list[analyte_list[0]].index))
#    for k in data_list[y].items():
#        print(k[0])
            
#    print(data_list[y].items())
#    print(data_list[y].index)
# Fill in title
#    for analyte in analyte_list:
       # Comp ID
    link_to_comp_ID = link_to[f"B{row-1}"]
    link_to_comp_ID.value="Composite Sample ID"
        # Samp ID
    link_to_samp_ID = link_to[f"C{row-1}"]
    link_to_samp_ID.value = "Sample ID"
        # Sample Date
    link_to_date = link_to[f"D{row-1}"]
    link_to_date.value="Depth (bgs)"
        # Depth
    link_to_depth = link_to[f"E{row-1}"]
    link_to_depth.value="Sample Date"
    
    for k in data_list[y].items():
#        print(k[0])
        link_to_comp_ID = link_to[f"B{row}"]
        link_to_comp_ID.value=k[0]
        link_to_samp_ID = link_to[f"C{row}"]
        link_to_samp_ID.value = k[0]
        row=row+1
        if len(k[0])>column_width:
            column_width=len(k[0])
    
#    for 
#    if (name=="SVOCs"):
#        print ("E16")

                

    for number in len_list:
        row = number + result_header_row
        link_list.append(row)
        
 #       print("link list row:" + str(row))
 #       print(link_list)
        
 #   for row in link_list:
        # Comp ID
 #       link_to_comp_ID = link_to[f"B{row}"]
 #       link_to_comp_ID.value="1" + str(row)
#        link_to_comp_ID.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,2,FALSE)"
        # Samp ID
 #       link_to_samp_ID = link_to[f"C{row}"]
 #       link_to_samp_ID.value = "2" + str(row)
 #       link_to_samp_ID.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,3,FALSE)"
        # Sample Date
 #       link_to_date = link_to[f"D{row}"]
 #       link_to_date.value="3" + str(row)
 #       link_to_date.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,5,FALSE)"
        # Depth
 #       link_to_depth = link_to[f"E{row}"]
 #       link_to_depth.value="4" + str(row)
 #       link_to_depth.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,4,FALSE)"

    center_cell(sheet, results_info_range)
    wrap_headers(sheet, result_header_row, intro_cols, 15)
    # set the height of the header row
    sheet.row_dimensions[result_header_row].height = 30
    # set the width of column's B-F (project info)
    sheet.column_dimensions["B"].width = 12
    sheet.column_dimensions["C"].width = column_width
    sheet.column_dimensions["D"].width = 12
    sheet.column_dimensions["E"].width = 7
# fill data here
#    df_col_len = len(final_output_tables[name].columns) + intro_cols
#    df_col_len_letter = get_column_letter(df_col_len)
#    col_analyte=len(final_output_tables[name].columns)
#    length_samples = len(output_tables[name])
#    print("column letter")
#    print(df_col_len_letter)
#    data_list=output_tables[name]
#    analyte_list=list(data_list)
#    samplelist=[]
#    y=analyte_list[0]

#    print("entering here")
    lastCol=FindEndCol(sheet,first_analyte_header_row,6)
#    print(lastCol)
    row = 1 + result_header_row
    initial_data_row=row
    columnNo=startDataResult_col
#    if (name=="SVOCs"):
#        print ("E17")
    
#    for key,value in analyte_name_to_display_name.items():
#        print(key,value)
    for analyte in analyte_list:
#        print("analyte:" + analyte)
#        if(name=="SVOCs"):
#            print("analyte:" + analyte)
        DName=analyte_name_to_display_name.get(analyte)
#        if(name=="SVOCs"):
        if DName==None:
           DName=analyte
#           print("Group:" + name +"DName - None:" + DName)
        #        print(analyte_name_to_display_name.get('Lead'))
#        print(analyte)
#        if DName=='None':
#            DName=analyte
#        print("1")
#        print("DNAME : "+DName)
#        print('2')
#        lastCol=FindEndCol(sheet,first_analyte_header_row)
#        print(lastCol)
        if analyte.find('Other')==-1:
#            print("go to sub :" + DName)
            fCol_letter=GetColLetter(sheet,DName,first_analyte_header_row,6,lastCol)
#            print(fCol_letter)
        else:
            fCol_letter=get_column_letter(df_col_len)
 #       print(analyte + " return: " +fCol_letter)

        row=initial_data_row

        for k in data_list[analyte].items():
            link_to_Data = link_to[f"{fCol_letter}{row}"]
            link_to_Data.value = k[1]
 #           print(k[1])
            row=row+1
#    if (name=="SVOCs"):
#        print ("E18")
        
            
 

    #### Apply conditional formatting ####
    # NOTE: order of rules matters!

    # Formatting Font of Results
    red_color = "ff0000"
    red_font = styles.Font(size=11, bold=True, color=red_color)
    orange_color = "ff8800"
    orange_font = styles.Font(size=11, bold=True, color=orange_color)
    yellow_color = "ffdd00"
    yellow_font = styles.Font(size=11, bold=True, color=yellow_color)
    blue_color = "1738E3"
    blue_color_font = styles.Font(size=11, bold=True, color=blue_color)
    bold_black_font = styles.Font(size=11, bold=True, color="000000")
    black_font = styles.Font(size=11, bold=False, color="000000")

    # if a result is not a number (i.e. a RL) don't bold/format
    sheet.conditional_formatting.add(
        # the range
        results_range,
        # the rule
        FormulaRule(
            formula=[f"NOT(ISNUMBER({first_result_loc}))"],
            stopIfTrue=True,
            font=black_font,
        ),
    )
    # if Hazardous Waste Criteria are being used
    if len(chosen_regulatory_criteria["Hazardous Waste Criteria"]) > 0:
        # if ADD-ON exceeds WET, turn ORANGE
        sheet.conditional_formatting.add(
            # the range
            results_range,
            # the rule
            FormulaRule(
                formula=[
                    f'AND(OR({first_result_loc}={STLC_loc},{first_result_loc}>{STLC_loc}),ISNUMBER(FIND("WET",{first_analyte_header_loc})))'
                ],
                stopIfTrue=True,
                font=orange_font,
            ),
        )
        #  if ADD-ON exceeds TCLP, turn RED
        sheet.conditional_formatting.add(
            # the range
            results_range,
            # the rule
            FormulaRule(
                formula=[
                    f'AND(OR({first_result_loc}={TCLP_loc},{first_result_loc}>{TCLP_loc}),ISNUMBER(FIND("TCLP",{first_analyte_header_loc})))'
                ],
                stopIfTrue=True,
                font=red_font,
            ),
        )
        # if RESULT exceeds TTLC, turn RED
        sheet.conditional_formatting.add(
            # the range
            results_range,
            # the rule
            FormulaRule(
                formula=[
                    f"OR({first_result_loc}={TTLC_loc},{first_result_loc}>{TTLC_loc})"
                ],
                stopIfTrue=True,
                font=red_font,
            ),
        )
    if len(chosen_regulatory_criteria["Risk Regulatory and Reference Criteria"]) > 0:
        # turn result blue and bold (Health Criteria)
        sheet.conditional_formatting.add(
            # the range
            results_range,
            # the rule
            FormulaRule(
                # AND makes sure that there are some numbers in health_min_range, otherwise erroneously evaluates to true
                formula=[
                    f'=AND((SUM({health_min_range})<>0),OR({first_result_loc}=MIN({health_min_range}),{first_result_loc}>MIN({health_min_range})))'
                ],
                stopIfTrue=False,
                font=blue_color_font,
            ),
        )
    # if a result is a digit (not a RL) bold result
    sheet.conditional_formatting.add(
        # the range
        results_range,
        # the rule
        FormulaRule(
            formula=[f"ISNUMBER({first_result_loc})"],
            stopIfTrue=False,
            font=bold_black_font,
        ),
    )




def format_gwtables(
    sheet, name, output_tables, final_output_tables, chosen_regulatory_criteria, gw_health_min_range_rows
):
    # delete random blank row
    sheet.delete_rows(2)

    ### DEFINE SHEET SPECIFIC PARAMETERS ###
    intro_cols = 4
    first_analyte_header_loc = "E$1"
    df_col_len = len(final_output_tables[name].columns) + intro_cols
    df_col_len_letter = get_column_letter(df_col_len)
    length_samples = len(output_tables[name])
    header_len = 2  # analyte name row and units row
    first_data_col_letter = "E"
    color_col_number = df_col_len + 1  # one after the last col in the table
    color_col = get_column_letter(color_col_number)
    ordering_col_number = color_col_number + 1
    ordering_col = get_column_letter(ordering_col_number)
    gw_health_min_range_list = [f"F${num}" for num in gw_health_min_range_rows]
    gw_health_min_range_str = ",".join(gw_health_min_range_list)


    # Criteria Info
    ## Risk and Regulatory (for GW) Criteria
    if (
        len(chosen_regulatory_criteria["Risk Regulatory and Reference Criteria (GW)"])
        > 0
    ):
        gw_risk_and_reg_crit_len = (
            len(
                chosen_regulatory_criteria[
                    "Risk Regulatory and Reference Criteria (GW)"
                ]
            )
            + 1
        )
        gw_risk_reg_pre_groups = header_len
        gw_risk_and_reg_crit_header_loc = f"B{gw_risk_reg_pre_groups + 1}"
        gw_risk_start_row = gw_risk_reg_pre_groups + 2
        gw_risk_and_reg_crit_first_loc = f"B{gw_risk_start_row}"
        gw_risk_stop_row = gw_risk_reg_pre_groups + gw_risk_and_reg_crit_len
        gw_risk_and_reg_crit_last_loc = f"{df_col_len_letter}{gw_risk_stop_row}"
        gw_risk_range = (
            f"{gw_risk_and_reg_crit_first_loc}:{gw_risk_and_reg_crit_last_loc}"
        )
        gw_risk_color_loc = f"{color_col}{gw_risk_reg_pre_groups + 2}"
        gw_health_min_range = gw_health_min_range_str
    else:
        gw_risk_and_reg_crit_len = 0
    ## Wastewater Discharge Criteria
    if len(chosen_regulatory_criteria["Wastewater Discharge Criteria"]) > 0:
        wastewater_crit_len = (
            len(chosen_regulatory_criteria["Wastewater Discharge Criteria"]) + 1
        )
        wastewater_crit_pre_groups = header_len + gw_risk_and_reg_crit_len
        wastewater_crit_header_loc = f"B{wastewater_crit_pre_groups + 1}"
        wastewater_start_row = wastewater_crit_pre_groups + 2
        wastewater_crit_first_loc = f"B{wastewater_start_row}"
        wastewater_stop_row = wastewater_crit_pre_groups + wastewater_crit_len
        wastewater_crit_last_loc = f"{df_col_len_letter}{wastewater_stop_row}"
        wastewater_range = f"{wastewater_crit_first_loc}:{wastewater_crit_last_loc}"
        wastewater_color_loc = f"{color_col}{wastewater_crit_pre_groups + 2}"
    else:
        wastewater_crit_len = 0

    # setting up data info
    reg_criteria_len = gw_risk_and_reg_crit_len + wastewater_crit_len

    # results info
    last_crit_row = header_len + reg_criteria_len
    result_header_row = header_len + reg_criteria_len + 1
    results_start_row = result_header_row + 1
    first_result_loc = f"{first_data_col_letter}{results_start_row}"
    results_stop_row = result_header_row + length_samples
    last_result_loc = f"{df_col_len_letter}{results_stop_row}"
    anchor_row = f"A{result_header_row}"
    results_info_1 = f"B{result_header_row}"
    results_info_2 = f"C{result_header_row}"
    results_info_3 = f"D{result_header_row}"
    results_info_range = f"B{result_header_row}:D{results_stop_row}"
    results_plusT1_range = f"B{results_start_row}:{last_result_loc}"
    results_range = f"{first_result_loc}:{last_result_loc}"
    results_color_loc = f"{color_col}{results_start_row}"
    ### END OF PARAMETERS ###

    # insert a col left of table to anchor the tables to, using a duplicate Sample ID column
    sheet.insert_cols(1)
    for cell in sheet[f"B:B"]:
        sheet.cell(row=cell.row, column=1, value=cell.value)

    # add blank columns to the left of data (2 columns - no depth for GW)
    sheet.insert_cols(3, 2)

    # center all values in cells
    center_cell(sheet, f"{first_data_col_letter}1:{last_result_loc}")

    ## FORMAT SECTION HEADINGS

    # CRITERIA HEADINGS

    # GW Risk Regulatory Criteria Heading
    if (
        len(chosen_regulatory_criteria["Risk Regulatory and Reference Criteria (GW)"])
        > 0
    ):
        sheet.insert_rows(header_len + 1)
        sheet[
            gw_risk_and_reg_crit_header_loc
        ] = "Risk Regulatory and Reference Criteria (GW)"
        set_header_format(sheet, f'{gw_risk_and_reg_crit_header_loc}:{df_col_len_letter}{gw_risk_reg_pre_groups + 1}')
        sheet[gw_risk_and_reg_crit_header_loc].border = Border(left=thin_black_border)
        sheet[f'{df_col_len_letter}{gw_risk_reg_pre_groups + 1}'].border = Border(right=thin_black_border)
        set_border(sheet,gw_risk_range)
        alt_numbers_col(color_col_number+1, gw_risk_start_row, gw_risk_stop_row, sheet)
        alt_row_color(gw_risk_range, gw_risk_color_loc, sheet)
        # merge criteria rows
        len_list = createList(gw_risk_and_reg_crit_len - 1)
        merge_list = []
        for number in len_list:
            row = f"B{number+ gw_risk_reg_pre_groups + 1}:D{number+ gw_risk_reg_pre_groups + 1}"
            merge_list.append(row)
        for location in merge_list:
            sheet.merge_cells(location)
            center_right_cell(sheet, location)

    # Wastewater Criteria Heading
    if len(chosen_regulatory_criteria["Wastewater Discharge Criteria"]) > 0:
        sheet.insert_rows(header_len + gw_risk_and_reg_crit_len + 1)
        sheet[wastewater_crit_header_loc] = "Wastewater Discharge Criteria"
        set_header_format(sheet, f'{wastewater_crit_header_loc}:{df_col_len_letter}{wastewater_crit_pre_groups + 1}')
        sheet[wastewater_crit_header_loc].border = Border(left=thin_black_border)
        sheet[f'{df_col_len_letter}{wastewater_crit_pre_groups + 1}'].border = Border(right=thin_black_border)
        set_border(sheet,wastewater_range)
        alt_numbers_col(color_col_number, wastewater_start_row, wastewater_stop_row, sheet)
        alt_row_color(wastewater_range, wastewater_color_loc, sheet)
        # merge criteria rows
        len_list = createList(wastewater_crit_len - 1)
        merge_list = []
        for number in len_list:
            row = f"B{number+ wastewater_crit_pre_groups + 1}:D{number+ wastewater_crit_pre_groups + 1}"
            merge_list.append(row)
        for location in merge_list:
            sheet.merge_cells(location)
            center_right_cell(sheet, location)

    # FORMAT TABLE

    # space column width according to largest cell
    set_col_width(sheet)

    # merge top left cell
    sheet.merge_cells("B1:D2")
    # styling header rows
    units_analytes_cells = f"B1:{df_col_len_letter}{header_len}"
    # add 'units' cell to heading
    sheet["B1"].value = "Units"
    sheet["B1"].alignment = Alignment(horizontal="right", vertical="bottom")
    # format 'units' merged cell background
    grey_fill = PatternFill(end_color="d0cece", start_color="d0cece", fill_type="solid")
    sheet["B1"].fill = grey_fill
    # style analyte and unit rows
    set_grey_fill(sheet, units_analytes_cells)
    set_border(sheet,units_analytes_cells)

    # Results Heading Row
    sheet.insert_rows(result_header_row)
    set_grey_fill(sheet, f'B{result_header_row}:{df_col_len_letter}{result_header_row}')
    set_border(sheet,f'B{result_header_row}:D{result_header_row}')
    sheet[f'{df_col_len_letter}{result_header_row}'].border = Border(right=thin_black_border)

    # fill alternating background color for results
    alt_numbers_col(color_col_number, results_start_row, results_stop_row, sheet)
    alt_row_color(results_plusT1_range, results_color_loc, sheet)
    set_border(sheet, results_plusT1_range)
    # hide background color column
    sheet.column_dimensions[f"{color_col}"].hidden = True
    # delete unnecessary cells in anchor col
    sheet[anchor_row].value = "Anchor"
    for row in sheet[f"A2:A{last_crit_row}"]:
        for cell in row:
            cell.value = None
    # hide anchor colomn
    sheet.column_dimensions["A"].hidden = True

    # Link Info from Table 1
    # add proj info to all results rows / Link Results Info from Table 1
    link_from = "Table 1 List"
    link_to = sheet
    len_list = createList(length_samples)
    link_list = []

    # link table 1 info headers
    link_to_comp_ID = link_to[results_info_1]
    link_to_comp_ID.value = f"='{link_from}'!B1"

    link_to_samp_ID = link_to[results_info_2]
    link_to_samp_ID.value = f"='{link_from}'!C1"

    link_to_date = link_to[results_info_3]
    link_to_date.value = f"='{link_from}'!E1"

    # link table 1 info by row, using v-lookup
    # important to use v-lookup incase the order changes
    for number in len_list:
        row = number + result_header_row
        link_list.append(row)

    for row in link_list:
        # Comp ID
        link_to_comp_ID = link_to[f"B{row}"]
        link_to_comp_ID.value = f"=VLOOKUP(A{row},'Table 1 List'!A:E,2,FALSE)"
        # Samp ID
        link_to_samp_ID = link_to[f"C{row}"]
        link_to_samp_ID.value = f"=VLOOKUP(A{row},'Table 1 List'!A:E,3,FALSE)"
        # Sample Date
        link_to_date = link_to[f"D{row}"]
        link_to_date.value = f"=VLOOKUP(A{row},'Table 1 List'!A:E,5,FALSE)"

    center_cell(sheet, results_info_range)
    wrap_headers(sheet, result_header_row, intro_cols, 15)
    # set the height of the header row
    sheet.row_dimensions[result_header_row].height = 30
    # set the width of column's B-D (project info)
    sheet.column_dimensions["B"].width = 10
    sheet.column_dimensions["C"].width = 15
    sheet.column_dimensions["D"].width = 12

    #### Apply conditional formatting ####
    # NOTE: order of rules matters!

    # Formatting Font of Results
    blue_color = "1738E3"
    blue_color_font = styles.Font(size=11, bold=True, color=blue_color)
    bold_black_font = styles.Font(size=11, bold=True, color="000000")
    black_font = styles.Font(size=11, bold=False, color="000000")

    # if a result is not a number (i.e. a RL) don't bold/format
    sheet.conditional_formatting.add(
        # the range
        results_range,
        # the rule
        FormulaRule(
            formula=[f"NOT(ISNUMBER({first_result_loc}))"],
            stopIfTrue=True,
            font=black_font,
        ),
    )
    if (
        len(chosen_regulatory_criteria["Risk Regulatory and Reference Criteria (GW)"])
        > 0
    ):
        # turn result blue and bold (Health Criteria)
        sheet.conditional_formatting.add(
            # the range
            results_range,
            # the rule
            FormulaRule(
                formula=[
                    f'=AND((SUM({gw_health_min_range})<>0),OR({first_result_loc}=MIN({gw_health_min_range}),{first_result_loc}>MIN({gw_health_min_range})))'
                ],
                stopIfTrue=False,
                font=blue_color_font,
            ),
        )
    # if a result is a digit (not a RL) bold result
    sheet.conditional_formatting.add(
        # the range
        results_range,
        # the rule
        FormulaRule(
            formula=[f"ISNUMBER({first_result_loc})"],
            stopIfTrue=False,
            font=bold_black_font,
        ),
    )

def format_soilvapor_tables(
    sheet, name, output_tables, final_output_tables, chosen_regulatory_criteria, sv_health_min_range_rows
):
    # delete random blank row
    sheet.delete_rows(2)

    ### DEFINE SHEET SPECIFIC PARAMETERS ###
    intro_cols = 4+1 #add an extra col to account for anchor col
    df_col_len = len(final_output_tables[name].columns)+intro_cols
    df_col_len_letter = get_column_letter(df_col_len)
    length_samples = len(output_tables[name])
    first_data_col_letter = 'F'
    color_col_number = len(final_output_tables[name].columns)+intro_cols+1
    color_col = get_column_letter(color_col_number)
    header_len = 2 #analyte name row and units row
    first_analyte_header_loc = 'F$1'
    sv_health_min_range_list = [f"F${num}" for num in sv_health_min_range_rows]
    sv_health_min_range_str = ",".join(sv_health_min_range_list)

    # Criteria Info
    ## Risk and Regulatory Criteria
    if len(chosen_regulatory_criteria['Risk Regulatory and Reference Criteria (SV)']) > 0:
        sv_risk_and_reg_crit_len = len(chosen_regulatory_criteria['Risk Regulatory and Reference Criteria (SV)']) + 1
        sv_risk_reg_pre_groups = header_len #all of the other regulatory groups that come before each reg. group
        sv_risk_and_reg_crit_header_loc = f'B{sv_risk_reg_pre_groups + 1}'
        sv_risk_and_reg_start_row = sv_risk_reg_pre_groups + 2
        sv_risk_and_reg_crit_first_loc = f'B{sv_risk_and_reg_start_row}'
        sv_risk_and_reg_stop_row = sv_risk_reg_pre_groups + sv_risk_and_reg_crit_len
        sv_risk_and_reg_crit_last_loc = f'{df_col_len_letter}{sv_risk_and_reg_stop_row}'
        sv_risk_range = f'{sv_risk_and_reg_crit_first_loc}:{sv_risk_and_reg_crit_last_loc}'
        sv_risk_color_loc = f'{color_col}{sv_risk_reg_pre_groups + 2}'
        sv_health_min_range = sv_health_min_range_str
    else:
        sv_risk_and_reg_crit_len = 0
    
    # data info
    sv_reg_criteria_len = sv_risk_and_reg_crit_len
    # stats info
    stats_pregroups = header_len + sv_reg_criteria_len
    stats_heading_loc = f'B{stats_pregroups + 1}'
    stats_len = 4
    stats_start_row = stats_pregroups+2
    stats_stop_row = stats_pregroups+4
    stats_range = f'B{stats_start_row}:{df_col_len_letter}{stats_stop_row}' 
    stats_color_loc = f'{color_col}{stats_pregroups+2}'
    # results info
    result_header_row = header_len + sv_reg_criteria_len + stats_len + 1
    results_start_row = result_header_row + 1
    first_result_loc = f'{first_data_col_letter}{results_start_row}'
    results_stop_row = result_header_row + length_samples
    last_result_loc = f'{df_col_len_letter}{results_stop_row}'
    anchor_row = f'A{result_header_row}'
    results_info_1 = f'B{result_header_row}'
    results_info_2 = f'C{result_header_row}'
    results_info_3 = f'D{result_header_row}'
    results_info_4 = f'E{result_header_row}'
    results_info_range = f'B{result_header_row}:E{results_stop_row}'   
    results_plusT1_range = f'B{results_start_row}:{last_result_loc}'
    results_range = f'{first_result_loc}:{last_result_loc}'
    results_color_loc = f'{color_col}{results_start_row}'
    ### END OF PARAMETERS ###
    
    ### Add rows + columns before formatting ###
    # insert a col left of table to anchor the tables to, using a duplicate Sample ID column
    sheet.insert_cols(1)
    for cell in sheet[f'B:B']:
        sheet.cell(row=cell.row, column=1, value=cell.value)
    
    # add blank columns to the left of data (3 columns)
    sheet.insert_cols(3,3)
    # center all values in cells
    center_cell(sheet, f'{first_data_col_letter}1:{last_result_loc}') 

    
    ### FORMAT TABLES ###

    # CRITERIA HEADINGS
    
    # Risk Regulatory Criteria Heading
    if len(chosen_regulatory_criteria['Risk Regulatory and Reference Criteria (SV)']) > 0:
        sheet.insert_rows(header_len + 1) #insert row (before existing row 3)
        sheet[sv_risk_and_reg_crit_header_loc] = 'Risk Regulatory and Reference Criteria'
        set_header_format(sheet, f'{sv_risk_and_reg_crit_header_loc}:{df_col_len_letter}{sv_risk_reg_pre_groups + 1}')
        sheet[sv_risk_and_reg_crit_header_loc].border = Border(left=thin_black_border)
        sheet[f'{df_col_len_letter}{sv_risk_reg_pre_groups + 1}'].border = Border(right=thin_black_border)
        set_border(sheet, sv_risk_range)
        alt_numbers_col(color_col_number, sv_risk_and_reg_start_row, sv_risk_and_reg_stop_row, sheet)
        alt_row_color(sv_risk_range, sv_risk_color_loc, sheet)
        #merge criteria rows
        len_list = createList(sv_risk_and_reg_crit_len-1)
        merge_list = []
        for number in len_list:
            row = f'B{number+ sv_risk_reg_pre_groups + 1}:E{number+ sv_risk_reg_pre_groups + 1}'
            merge_list.append(row)
        for location in merge_list:
            sheet.merge_cells(location)
            center_right_cell(sheet, location)
            
    # Stats Heading Row
    sheet.insert_rows(stats_pregroups + 1)
    sheet[stats_heading_loc] = 'Statistical Evaluation'
    set_header_format(sheet, f'{stats_heading_loc}:{df_col_len_letter}{stats_pregroups + 1}')
    sheet[stats_heading_loc].border = Border(left=thin_black_border)
    sheet[f'{df_col_len_letter}{stats_pregroups + 1}'].border = Border(right=thin_black_border)
    set_border(sheet, stats_range)
    alt_numbers_col(color_col_number, stats_start_row, stats_stop_row, sheet)        
    alt_row_color(stats_range, stats_color_loc, sheet)  
    # merge stats rows
    len_list = createList(3)
    merge_list = []
    for number in len_list:
        row = f'B{number+ header_len+sv_reg_criteria_len + 1}:E{number+ header_len+sv_reg_criteria_len + 1}'
        merge_list.append(row)
    for location in merge_list:
        sheet.merge_cells(location)
        center_right_cell(sheet, location)
    
    # space column width according to largest cell
    set_col_width(sheet)
            
    # merge top left cell
    sheet.merge_cells(f'B1:E{header_len}')
    # styling header rows
    units_analytes_cells = f'B1:{df_col_len_letter}{header_len}'
    # add 'units' cell to heading
    sheet['B1'].value = 'Units'
    sheet['B1'].alignment = Alignment(horizontal='right',
                        vertical='bottom')
    # format 'units' merged cell background
    grey_fill =  PatternFill(end_color = "d0cece", start_color="d0cece", fill_type = "solid")
    sheet['B1'].fill = grey_fill
    # style analyte and unit rows
    set_grey_fill(sheet, units_analytes_cells)
    set_border(sheet,units_analytes_cells)
    
    # Results Heading Row
    sheet.insert_rows(result_header_row)
    set_grey_fill(sheet, f'B{result_header_row}:{df_col_len_letter}{result_header_row}')
    set_border(sheet,f'B{result_header_row}:E{result_header_row}')
    sheet[f'{df_col_len_letter}{result_header_row}'].border = Border(right=thin_black_border)
    
    # fill alternating background color for results
    alt_numbers_col(color_col_number, results_start_row, results_stop_row, sheet)
    alt_row_color(results_plusT1_range, results_color_loc, sheet)
    set_border(sheet, results_plusT1_range)
    # hide background color column
    sheet.column_dimensions[f'{color_col}'].hidden= True
    # delete unnecessary cells in anchor col
    sheet[anchor_row].value = 'Anchor'
    for row in sheet[f'A2:A{stats_stop_row}']:
        for cell in row:
            cell.value = None
    # hide anchor colomn
    sheet.column_dimensions['A'].hidden= True
    
    # Link Info from Table 1
    # add proj info to all results rows / Link Results Info from Table 1
    link_from = 'Table 1 List'
    link_to = sheet
    len_list = createList(length_samples)
    link_list = []
    
    # link table 1 info HEADERS
    link_to_comp_ID = link_to[results_info_1]
    link_to_comp_ID.value = f"='{link_from}'!B1"
    link_to_comp_ID.alignment = Alignment(wrapText=True,horizontal='center', vertical='center')

    link_to_samp_ID = link_to[results_info_2]
    link_to_samp_ID.value = f"='{link_from}'!C1"
    link_to_samp_ID.alignment = Alignment(wrapText=True,horizontal='center', vertical='center')
    
    link_to_date = link_to[results_info_3]
    link_to_date.value = f"='{link_from}'!E1"
    link_to_date.alignment = Alignment(wrapText=True,horizontal='center', vertical='center')

    link_to_depth = link_to[results_info_4]
    link_to_depth.value = f"='{link_from}'!D1"
    link_to_depth.alignment = Alignment(wrapText=True,horizontal='center', vertical='center')

    # link table 1 info by row, using v-lookup
    # important to use v-lookup incase the order changes
    for number in len_list:
        row = number + result_header_row
        link_list.append(row)

    for row in link_list:
        # Comp ID
        link_to_comp_ID = link_to[f'B{row}']
        link_to_comp_ID.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,2,FALSE)"
        # Samp ID
        link_to_samp_ID = link_to[f'C{row}']
        link_to_samp_ID.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,3,FALSE)"
        # Sample Date
        link_to_date = link_to[f'D{row}']
        link_to_date.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,5,FALSE)"
        # Depth
        link_to_depth = link_to[f'E{row}']
        link_to_depth.value = f"=VLOOKUP(A{row},'Table 1 List'!A:F,4,FALSE)"
        
    center_cell(sheet, results_info_range)
    wrap_headers(sheet, result_header_row, intro_cols,15)
    # set the height of the header row 
    sheet.row_dimensions[result_header_row].height = 30
   # set the width of column's B-F (project info)
    sheet.column_dimensions['B'].width = 10
    sheet.column_dimensions['C'].width = 15
    sheet.column_dimensions['D'].width = 12
    sheet.column_dimensions['E'].width = 7     
    
    
    #### Apply conditional formatting ####
    # NOTE: order of rules matters!
    
    # Formatting Font of Results
    blue_color = '1738E3'
    blue_color_font = styles.Font(size=11, bold=True, color=blue_color)
    bold_black_font = styles.Font(size=11, bold=True, color='000000')
    black_font =  styles.Font(size=11, bold=False, color='000000')

    # if a result is not a number (i.e. a RL) don't bold/format
    sheet.conditional_formatting.add(
        # the range
         results_range,
        # the rule
        FormulaRule(formula=[f'NOT(ISNUMBER({first_result_loc}))'],
                    stopIfTrue=True,
                    font=black_font
        )
    )
    if len(chosen_regulatory_criteria['Risk Regulatory and Reference Criteria (SV)']) > 0:
        # turn result blue and bold (Health Criteria)
        sheet.conditional_formatting.add(
            # the range
            results_range,
            # the rule
            FormulaRule(
                # AND makes sure that there are some numbers in health_min_range, otherwise erroneously evaluates to true
                formula=[f'=AND((SUM({sv_health_min_range})<>0),OR({first_result_loc}=MIN({sv_health_min_range}),{first_result_loc}>MIN({sv_health_min_range})))'],
                stopIfTrue=False,
                font=blue_color_font
            )
        ) 
    # if a result is a digit (not a RL) bold result
    sheet.conditional_formatting.add(
        # the range
        results_range,
        # the rule
        FormulaRule(formula=[f'ISNUMBER({first_result_loc})'],
                    stopIfTrue=False,
                    font=bold_black_font)
        )


def format_footnotestables(sheet, name, footnotes_tables):
    # italicize and underline header row
    footnotes_heading_font = styles.Font(
        size=11, underline="single", italic=True, color="000000"
    )
    sheet["A1"].value = "General Notes:"
    sheet["A1"].font = footnotes_heading_font
    # other rows to italicize and underline
    rowstoformat = [
        "Regulatory Criteria:",
        "Hazardous Waste and Recycling Facility Acceptance Criteria:",
    ]
    df = footnotes_tables[name]
    for row in rowstoformat:
        rowindex = df[df[name] == row].index
        # some tables dont have "regulatory criteria:"
        if len(rowindex) > 0:
            rownumber = rowindex[0] + 2
            sheet[f"A{rownumber}"].font = footnotes_heading_font


def format_page_layout(sheet, name, sorted_analytegroup_list, sorted_analytegroup_footnotes_list):
    # make tables landscape
    openpyxl.worksheet.worksheet.Worksheet.set_printer_settings(
        sheet, paper_size=12, orientation="landscape"
    )
#    print(sorted_analytegroup_list)
#    print("Sorted Analyte Group List : " +sorted_analytegroup_list)
#    print("Input Name is " + name)
    # Table Header
    header_size = 11
    header_font = "Calibri,Bold"
    headerfooter_color = "00325e"
 #   print("Name is :" + name)
    # Associate Table Name with position in list according to index
 #   for i in sorted_analytegroup_list:
 #       print("name : ")
 #       print(i)
 #       print(" index: ")
 #      print(sorted_analytegroup_list.index(i))
        
    if name == "Table 1 List":
        index = sorted_analytegroup_list.index(name)
#        print("Index of Table 1: " + str(index))
    if name in sorted_analytegroup_list:
        index = sorted_analytegroup_list.index(name)
   #     print("Index of " + name + ": " + str(index))
    if name in sorted_analytegroup_footnotes_list:
        index = sorted_analytegroup_footnotes_list.index(name)
        #print("Index of " + name + " in list of footnotes : " + str(index))
    # Call name of each table from dictionary
    header_name = analytegroup_table_headings[name.replace(" Notes", "")]
    sheet.HeaderFooter.differentFirst = True
    
 #   print("Header Name is " + header_name)
 #   print("Index is "+ str(index))
    sheet.firstHeader.left.text = f"Table {index + 1}\n{header_name}"
    sheet.firstHeader.left.size = header_size
    sheet.firstHeader.left.font = header_font
    sheet.firstHeader.left.color = headerfooter_color

    sheet.oddHeader.left.text = f"Table {index + 1} (Cont'd)\n{header_name}"
    sheet.oddHeader.left.size = header_size
    sheet.oddHeader.left.font = header_font
    sheet.oddHeader.left.color = headerfooter_color

    # Table Footer
    sheet.oddFooter.left.text = "&F"
    sheet.oddFooter.left.size = 8
    sheet.oddFooter.left.font = "Calibri"
    sheet.oddFooter.left.color = headerfooter_color
    sheet.oddFooter.center.text = "Page &P of &N"
    sheet.oddFooter.center.size = 10
    sheet.oddFooter.center.font = "Calibri"
    sheet.oddFooter.center.color = headerfooter_color

    sheet.firstFooter.left.text = "&F"
    sheet.firstFooter.left.size = 8
    sheet.firstFooter.left.font = "Calibri"
    sheet.firstFooter.left.color = headerfooter_color
    sheet.firstFooter.center.text = "Page &P of &N"
    sheet.firstFooter.center.size = 10
    sheet.firstFooter.center.font = "Calibri"
    sheet.firstFooter.center.color = headerfooter_color

    # select columns to repeat on each page for printing
    if name in soil_tables_list:
        sheet.print_title_cols = "A:E"
    if name in gw_tables_list:
        sheet.print_title_cols = "A:D"
    if name in sv_tables_list:
        sheet.print_title_cols = "A:E"
    # Table 1
    if name == "Table 1 List":
        sheet.print_title_cols = "A:E"


def style_excel_tables(
    formatted_lab_data, 
    output_tables, 
    chosen_regulatory_criteria, 
    footnotes_tables, 
    health_min_range_rows, 
    gw_health_min_range_rows, 
    sv_health_min_range_rows, 
    len_soil_samples,
    len_gw_samples,
    len_sv_samples,
):

    final_output_tables = formatted_lab_data

    # order sheets in workbook according to dictionary: "numbered_output_tables"
    sorted_analytegroup_list = list(final_output_tables.keys())
    
   
    sorted_analytegroup_list.sort(
        key=lambda analyte_group: numbered_output_tables[analyte_group]
    )
#    print("sorted_analytegroup_list")
#    print(sorted_analytegroup_list)

    sorted_analytegroup_footnotes_list = list(footnotes_tables.keys())
    sorted_analytegroup_footnotes_list.sort(
        key=lambda analyte_group: numbered_output_tables[analyte_group]
    )
#    print("sorted_analytegroup_footnotes_list")
#    print(sorted_analytegroup_footnotes_list)

    # workbook object holds everything
    wb = Workbook()

    # Worksheet is like the sheet in an excel doc,
    # automatically has one worksheet when workbook is created, access it with wb.active

    dict_of_worksheets = {}
    # to load in dfs that are stored in a dictionary
    for analyte_group, df in final_output_tables.items():
        dict_of_worksheets[analyte_group] = wb.create_sheet(title=analyte_group)

        if analyte_group == "Table 1 List":
            for row in dataframe_to_rows(df, index=False, header=True):
                dict_of_worksheets[analyte_group].append(row)
        else:
            for row in dataframe_to_rows(df, index=True, header=True):
                dict_of_worksheets[analyte_group].append(row)

    # delete sheet automatically created when creating workbook
    wb.remove(wb["Sheet"])

    # add footnotes sheets and fill with appropriate footnotes by analyte group
    for footnotes_group, df in footnotes_tables.items():
        dict_of_worksheets[footnotes_group] = wb.create_sheet(title=footnotes_group)

        for row in dataframe_to_rows(df, index=False, header=True):
            dict_of_worksheets[footnotes_group].append(row)

    # to edit a worksheet, must reference ws within dictionary of ws
    for name, sheet in dict_of_worksheets.items():
        if name == "Table 1 List":
            format_table1(sheet, final_output_tables, len_soil_samples, len_gw_samples,len_sv_samples)
#        print("style excel function")
#        print(output_tables)
 #       print("Done")
        ##### FOR ALL SOIL TABLES #####
        if name in soil_tables_list:
 #           if (name=="SVOCs"):
 #               print("Soil :" + name)
            format_soiltables(
                sheet,
                name,
                output_tables,
                final_output_tables,
                chosen_regulatory_criteria,
                health_min_range_rows
            )

        ##### FOR ALL GW TABLES #####
        if name in gw_tables_list:
            format_gwtables(
                sheet,
                name,
                output_tables,
                final_output_tables,
                chosen_regulatory_criteria,
                gw_health_min_range_rows
            )

        ##### FOR ALL SV TABLES #####
        if name in sv_tables_list:
            format_soilvapor_tables(
                sheet, 
                name, 
                output_tables, 
                final_output_tables, 
                chosen_regulatory_criteria, 
                sv_health_min_range_rows
            )


        ##### FORMAT FOR FOOTNOTES SHEETS #####
        if name in list_of_footnotes_groups:
            format_footnotestables(sheet, name, footnotes_tables)

    ## Page Layout Settings
    #print("sheet :" + dict_of_worksheets.items() )
    for name, sheet in dict_of_worksheets.items():
 #       print("Format Name :" + name)
 #       print("sorted list :" + sorted_analytegroup_list)
 #       print("sorted footnote list :" + sorted_analytegroup_footnotes_list)
        format_page_layout(sheet, name, sorted_analytegroup_list, sorted_analytegroup_footnotes_list)

    # order sheets in workbook according to dictionary: "numbered_output_tables"
    wb._sheets.sort(key=lambda ws: numbered_output_tables[ws.title])

    # order sheets in workbook according to dictionary: "numbered_output_tables"
    wb._sheets.sort(key=lambda ws: numbered_output_tables[ws.title])
    # Save File
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output
