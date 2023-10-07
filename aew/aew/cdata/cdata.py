from unicodedata import category
from flask import Blueprint, request, render_template, flash, redirect, url_for, send_file, jsonify, session

import pandas as pd
import os
import pickle
import logging

from aew.cdata.cdataPandas import files_to_df

from .haz_waste_check import (
    generate_hazardous_waste_reports,
    format_and_export_haz_waste_analysis,
    format_and_export_haz_waste_add_on,)

from .reformat_lab_report import generate_formatted_lab_report, style_excel_tables, regulatory_category_dict, edd_validation

sample_names=[]


cdata_bp=Blueprint("cdata",__name__,template_folder="templates",
    static_folder="static",
    static_url_path="/cdata/static",
                   )

app_root_path = cdata_bp.root_path

# special case to deal with all haz waste at once
all_regulatory_preferences = ["all_haz_waste"]
for key, value in regulatory_category_dict.items():
    all_regulatory_preferences.extend(value)



def df_from_sample_order(sample_order):
    df = pd.DataFrame({"Sample ID":sample_order})
    df['Order'] = df.index + 1
    return df

def decode_sample_id(string):
    return string.replace("*SINQUO*", "'").replace("*DUBQUO*", '"').replace("*COMMA*", ",").replace("*AMPR*", '&')


@cdata_bp.route('/test')
def test():
    return render_template("test.html")

@cdata_bp.route('/cdataIndex')
def cdataIndex():
    return render_template("cdataIndex.html")

@cdata_bp.route('/cdataImport',methods=['GET', 'POST'])
def cdataImport():
    myLogIn=session.get('loggedin', None)
    
    if myLogIn==True:
        if request.method=='POST':
    # Input Lab Data
    # checks first fileobject filename
    #      print("FOUND THIS FOR UPLOADAED FILES:", request.files["lab_data_input"].filename)
            if request.files["lab_data_input"].filename == "":
              flash("No Data Uploaded. Please Upload Lab Data.", category="err")
              return redirect(url_for("cdataIndex.html"))
            else:
               lab_data_files = request.files.getlist("lab_data_input")
               lab_data = files_to_df(lab_data_files)
               flash("Data uploaded.", category="success")

               flash("No of Records : " + str(len(lab_data)) + " processed.", category="success")
                  
               return redirect(url_for('cdata.cdataIndex'))
        return render_template("cdataImport.html")
    return redirect(url_for('views.home'))

    
@cdata_bp.route('/cdataLeaching', methods=['GET','POST'])
def cdataLeaching():
    myLogIn=session.get('loggedin', None)

    if myLogIn==True:
        if request.method=='POST':
            print("FOUND THIS FOR UPLOADAED FILES:", request.files["lab_data_input"].filename)
            if request.files["lab_data_input"].filename == "":
               flash("No Data Uploaded. Please Upload Lab Data.", category="err")
               return redirect(url_for("cdataCheckLeaching"))
            else:
                lab_data_files = request.files.getlist("lab_data_input")
                lab_data = files_to_df(lab_data_files)
        
    # Input Reference Data
                regulatory_criteria_references_database = pd.read_excel(
                os.path.join(
                app_root_path, "static", "resources", "regulatory_criteria_references.xlsx"
                ),
                keep_default_na=False,
                )
                footnotes_df = pd.read_excel(
                os.path.join(app_root_path, "static", "resources", "footnotes.xlsx"),
                sheet_name="FootnotesDF",
                )

    # Download Haz Waste Results
        # Generate  Haz Waste Results
                haz_waste_add_ons, haz_waste_analysis = generate_hazardous_waste_reports(
                    lab_data, regulatory_criteria_references_database
                )
                output = format_and_export_haz_waste_add_on(haz_waste_add_ons)
                fname=request.form.get('fname')
#                print(fname)
                fname=fname+ "_leachingCheck.xlsx"
#                print (fname)
                return send_file(output, download_name=fname, as_attachment=True)

        return render_template("cdataLeaching.html")

    return redirect(url_for('views.home'))


@cdata_bp.route('/cdataGenerate', methods=['GET','POST'])
def cdataGenerate():
    myLogIn=session.get('loggedin', None)

    if myLogIn==True:
        user=session.get('tempuname',"None")
        if request.method=='POST':
            print("FOUND THIS FOR UPLOADAED FILES:", request.files["lab_data_input"].filename)
            if request.files["lab_data_input"].filename == "":
               flash("No Data Uploaded. Please Upload Lab Data.", category="err")
               return redirect(url_for("cdataCheckLeaching"))
            else:
                lab_data_files = request.files.getlist("lab_data_input")
                lab_data = files_to_df(lab_data_files)
                lab_data.to_pickle(f'./aew/cdata/temp_files/lab_data_{user}.pkl')
                sample_names=list(lab_data['SAMPID'].unique())
                flash("Data uploaded.", category="success")
                flash("No of Records : " + str(len(lab_data)) + " processed.", category="success")

                return redirect(url_for("cdata.cdataRpt"))
#                return redirect(url_for("cdataRpt", regulatory_category_dict=regulatory_category_dict))
 #               return render_template("cdataRpt.html",regulatory_category_dict=regulatory_category_dict)
        return render_template("cdataGenerate.html")

    return redirect(url_for('views.home'))

@cdata_bp.route('/cdataRpt', methods=['GET','POST'])
def cdataRpt():
    myLogIn=session.get('loggedin', None)

    if myLogIn==True:
        if request.method=='POST':
            user=session.get('tempuname',"None")
            lab_data = pd.read_pickle(f'./aew/cdata/temp_files/lab_data_{user}.pkl')
            sample_names=list(lab_data['SAMPID'].unique())
#            print(lab_data.to_string())
#            print("1")
    # Input Reference Data
            regulatory_criteria_references_database = pd.read_excel(
                os.path.join(
                app_root_path, "static", "resources", "regulatory_criteria_references.xlsx"
                ),
                keep_default_na=False,
                )
#            print("2")
            footnotes_df = pd.read_excel(
                os.path.join(app_root_path, "static", "resources", "footnotes.xlsx"),
                sheet_name="FootnotesDF",
                )
#            print("3")

     # Input regulatory preference 
            regulatory_preference = []
            for checkbox in all_regulatory_preferences:
                if request.form.get(checkbox):
                    regulatory_preference.append(checkbox)
#            print("4")
    # If all haz waste are selected, add them individually
            if "all_haz_waste" in regulatory_preference:
                regulatory_preference.extend(["TTLC", "STLCx10", "STLC", "TCLPx20", "TCLP"])
                regulatory_preference.remove("all_haz_waste")
#            print("5")

    # reorder chosen criteria so they are correct for openpyxl
            correct_order = regulatory_criteria_references_database.columns.to_list()
            regulatory_preference = [pref for pref in correct_order if pref in regulatory_preference]
#            print("Correct Order")
            #print(regulatory_preference)
    # Input sample order
            #print("sample name")
            #print(sample_names)
            sample_order_list = [decode_sample_id(sample) for sample in sample_names]
            sample_order_df = df_from_sample_order(sample_order_list)
            #print("sample order list")
            #print(sample_order_list)
            #print("sample order list df")
            #print(sample_order_df.to_string())
    ### TODO: INTEGRATE SAMPLE ORDER INTO lab data processing
 #           print("1")
            
            try:
        # Generate Formatted Lab Data with Regulatory Info
                    (
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
                    ) = generate_formatted_lab_report(
                        lab_data, regulatory_criteria_references_database, regulatory_preference, footnotes_df, sample_order_df
                    )
            except:
                logging.exception("message")
                flash(
                "There was an error processing this data at Generate Formatted Lab Report Module.",
                category="missing_data",
                )
            try:
                    #print("6")
                    output = style_excel_tables(
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
                    )
                    #print("7")
                    fname=request.form.get('fname')
                    if len(fname)>0:
                        fname=fname + ' Tables.xlsx'
                    else:
                        fname='Lab Report Tables.xlsx'
#                print (fname) 
                    return send_file(output, download_name=fname, as_attachment=True)
            except:
                logging.exception("message")
                flash(
                "There was an error processing this data at Style Excel Tables Module.",
                category="missing_data",
                )
            return render_template("cdataIndex.html")
        return render_template("cdataRpt.html",regulatory_category_dict=regulatory_category_dict)
    