from flask import Blueprint, request, render_template, flash, redirect, url_for, send_file, jsonify
from flask_login import login_required, current_user

import pandas as pd
import os
import pickle
import logging
from .haz_waste_check import (
    generate_hazardous_waste_reports,
    format_and_export_haz_waste_analysis,
    format_and_export_haz_waste_add_on,
)
from .reformat_lab_report import generate_formatted_lab_report, style_excel_tables, regulatory_category_dict, edd_validation


edd_processing_bp = Blueprint(
    "edd_processing",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/edd_processing/static",
)

app_root_path = edd_processing_bp.root_path

# special case to deal with all haz waste at once
all_regulatory_preferences = ["all_haz_waste"]
for key, value in regulatory_category_dict.items():
    all_regulatory_preferences.extend(value)


def files_to_df(uploaded_files_list):
    if uploaded_files_list:
        all_data = pd.DataFrame()
        for file in uploaded_files_list:
            data = edd_validation(file)
            all_data = all_data.append(data, ignore_index=True)
        return all_data.reset_index(drop=True)
    else:
        return None


@edd_processing_bp.route("/edd_processing")
@login_required
def main():
    return render_template(
        "edd_processing.html", regulatory_category_dict=regulatory_category_dict
    )


@edd_processing_bp.route("/table_generator_init", methods=["POST"])
@login_required
def table_generator_init():
    lab_data_files = request.files.getlist("lab_data_input")
    lab_data = files_to_df(lab_data_files)
    # Save lab data to disk for subsequent processing
    user = current_user.name
    lab_data.to_pickle(f'./aew_web_portal/edd_processing/temp_files/lab_data_{user}.pkl')
    # Send sample names back to front end for ordering
    sample_names = list(lab_data['SAMPID'].unique())
    return jsonify(sample_names)
   

def df_from_sample_order(sample_order):
    df = pd.DataFrame({"Sample ID":sample_order})
    df['Order'] = df.index + 1
    return df

def decode_sample_id(string):
    return string.replace("*SINQUO*", "'").replace("*DUBQUO*", '"').replace("*COMMA*", ",").replace("*AMPR*", '&')

@edd_processing_bp.route("/table_generator", methods=["POST"])
@login_required
def table_generator():
    # Input lab data
    user = current_user.name
    lab_data = pd.read_pickle(f'./aew_web_portal/edd_processing/temp_files/lab_data_{user}.pkl')

    # Input Reference Data
    regulatory_criteria_references_database = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vTtVj95lKdwUeb7rFfPZKX5n9jBcoR3-448epl_qSGL3ePT2FiPuuxcb_7VYlOLdPsCBE-s7nyMgIra/pub?gid=0&single=true&output=csv",
        keep_default_na=False,
    )
    footnotes_df = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vTtVj95lKdwUeb7rFfPZKX5n9jBcoR3-448epl_qSGL3ePT2FiPuuxcb_7VYlOLdPsCBE-s7nyMgIra/pub?gid=619142125&single=true&output=csv"
    )

     # Input regulatory preference 
    regulatory_preference = []
    for checkbox in all_regulatory_preferences:
        if request.form.get(checkbox):
            regulatory_preference.append(checkbox)

    # If all haz waste are selected, add them individually
    if "all_haz_waste" in regulatory_preference:
        regulatory_preference.extend(["TTLC", "STLCx10", "STLC", "TCLPx20", "TCLP"])
        regulatory_preference.remove("all_haz_waste")

    # reorder chosen criteria so they are correct for openpyxl
    correct_order = regulatory_criteria_references_database.columns.to_list()
    regulatory_preference = [pref for pref in correct_order if pref in regulatory_preference]

    # Input sample order
    sample_order_list = [decode_sample_id(sample) for sample in request.form['sample_order_field'].split(",") ]
    sample_order_df = df_from_sample_order(sample_order_list)
    ### TODO: INTEGRATE SAMPLE ORDER INTO lab data processing

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
        return send_file(output, attachment_filename='Lab Report Tables.xlsx', as_attachment=True)
    except:
        logging.exception("message")
        flash(
            "There was an error processing this data. Please contact web admin.",
            category="missing_data",
        )
        return redirect(url_for("edd_processing.main"))



@edd_processing_bp.route("/haz_waste_processing", methods=["POST"])
@login_required
def haz_waste_processing():
    # Input Lab Data
    # checks first fileobject filename
    print("FPUND THIS FOR UPLOADAED FILES:", request.files["lab_data_input"].filename)
    if request.files["lab_data_input"].filename == "":
        flash("No Data Uploaded. Please Upload Lab Data.", category="missing_data")
        return redirect(url_for("edd_processing.main"))
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
    if request.form["submit_button"] == "Download Hazardous Waste Check":
        # Generate  Haz Waste Results
        haz_waste_add_ons, haz_waste_analysis = generate_hazardous_waste_reports(
            lab_data, regulatory_criteria_references_database
        )
        output = format_and_export_haz_waste_analysis(haz_waste_analysis)
        return send_file(output, attachment_filename='Hazardous Waste Analysis.xlsx', as_attachment=True)

    # Download Haz Waste Add Ons
    if request.form["submit_button"] == "Download Potential Add-Ons":
        haz_waste_add_ons, haz_waste_analysis = generate_hazardous_waste_reports(
            lab_data, regulatory_criteria_references_database
        )
        output = format_and_export_haz_waste_add_on(haz_waste_add_ons)
        return send_file(output, attachment_filename='Hazardous Waste Add-Ons.xlsx', as_attachment=True)


