import re
import os
import csv
import json
import click
import logging
import subprocess
import paramiko
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
from workflows.vm_control import start_vm, stop_vm,check_vm_status
from workflows.utils.jira_utils import delete_ticket

colossus_api = ColossusApi()
tantalus_api = TantalusApi()
ana_list = colossus_api.list('analysis_information')

lib_list = []
lib_with_duplicate_ana = []

lib_ana_dict_firstround = {}
lib_ana_dict_duplicate = {}

duplicate_reasons = {"bug": [], "pipeline_update": [], "lane_update": [], "others": [], "aligner": [], "pipeline_downgrade": [], "diff_status": []}
duplicate_reasons_jira = {"bug": [], "pipeline_update": [], "lane_update": [], "others": [], "aligner": [], "pipeline_downgrade": [], "diff_status": []}


def make_lib_ana_duplicate_csv():
    with open('duplicate_info.csv', 'w+', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["lib_id", "analysis_id", "analysis_jira_ticket", "pipeline_version", "analysis_submission_date", "analysis_run_status", "aligner", "number_of_lanes"])
        writer.writerow(["", "", "", "", "", "", "", "", ""])
        for key, value in lib_ana_dict_duplicate.items():
            lib_id = key
            for analysis in value:
                analysis_id = analysis[0]
                analysis_jira_ticket = analysis[1]
                pipeline_version = analysis[2]
                analysis_submission_date = analysis[3]
                analysis_run_status = analysis[4]
                aligner = analysis[5]
                number_of_lanes = analysis[6]
                writer.writerow([lib_id, analysis_id, analysis_jira_ticket, pipeline_version, analysis_submission_date, analysis_run_status, aligner, number_of_lanes])
            writer.writerow(["", "", "", "", "", "", "", "", ""])

def list_dup_information():
    for ana in ana_list:
        curr_ana_info = []
        lib_id = 0
        for key, value in ana.items():
            if key == "id" or key == "analysis_jira_ticket" or key == "version" or key == "aligner" or key == "analysis_submission_date":
                curr_ana_info.append(value)
            elif key == "lanes":
                curr_ana_info.append(len(value))
            elif key == "analysis_run":
                for key2, value2 in value.items():
                    if key2 == "run_status":
                        curr_ana_info.append(value2)
                        break
            elif key == "library":
                for key2, value2 in value.items():
                    if key2 == "pool_id":
                        lib_id = value2
        if lib_id not in lib_ana_dict_firstround:
            lib_ana_dict_firstround[lib_id] = curr_ana_info
        elif lib_id not in lib_ana_dict_duplicate:
            curr_lib_ana_info = []
            curr_lib_ana_info.append(lib_ana_dict_firstround[lib_id])
            curr_lib_ana_info.append(curr_ana_info)
            lib_ana_dict_duplicate[lib_id] = curr_lib_ana_info
        else:
            lib_ana_dict_duplicate[lib_id].append(curr_ana_info)

def compare_version(ver1, ver2):
    ver1_split = ver1[1:].split(".")
    ver2_split = ver2[1:].split(".")
    for index in range(0, 3):
        if int(ver1_split[index]) > int(ver2_split[index]):
            return "larger"
        elif int(ver1_split[index]) < int(ver2_split[index]):
            return "smaller"
    return "equal"

    
def identify_dup_reason():
    for key, value in lib_ana_dict_duplicate.items():
        pivot_info = {}
        for analysis in value:
            if len(pivot_info) == 0:
                id = analysis[0]
                jira = analysis[1]
                pivot_info["pipeline_version"] = analysis[2]
                pivot_info["analysis_submission_date"] = analysis[3]
                pivot_info["analysis_run_status"] = analysis[4]
                pivot_info["aligner"] = analysis[5]
                pivot_info["number_of_lanes"] = analysis[6]
            else:
                version_compare = compare_version(analysis[2], pivot_info["pipeline_version"])
                if version_compare == "larger" and \
                analysis[6] == pivot_info["number_of_lanes"] and \
                analysis[4] == "complete" and \
                analysis[5] == pivot_info["aligner"]:
                    duplicate_reasons["pipeline_update"].append(id)
                    duplicate_reasons_jira["pipeline_update"].append(jira)
                elif analysis[6] > pivot_info["number_of_lanes"] and \
                analysis[5] == pivot_info["aligner"] and \
                analysis[4] == "complete" and \
                (version_compare == "equal" or version_compare == "larger"):
                    duplicate_reasons["lane_update"].append(id)
                    duplicate_reasons_jira["lane_update"].append(jira)
                elif version_compare == "equal" and \
                      analysis[4] == pivot_info["analysis_run_status"] and \
                      analysis[5] == pivot_info["aligner"] and \
                      analysis[6] == pivot_info["number_of_lanes"]:
                    duplicate_reasons["bug"].append(id)
                    duplicate_reasons_jira["bug"].append(jira)
                elif analysis[5] != pivot_info["aligner"] and \
                analysis[6] >= pivot_info["number_of_lanes"]:
                    if analysis[5] == "M":
                        duplicate_reasons["aligner"].append(id)
                        duplicate_reasons_jira["aligner"].append(jira)
                    elif pivot_info["aligner"] == "M":
                        duplicate_reasons["aligner"].append(analysis[0])
                        duplicate_reasons_jira["aligner"].append(analysis[1])
                elif pivot_info["analysis_run_status"] != analysis[4] and \
                analysis[5] == pivot_info["aligner"] and \
                analysis[6] == pivot_info["number_of_lanes"]:
                    if analysis[4] == "complete":
                        duplicate_reasons["diff_status"].append(id)
                        duplicate_reasons_jira["diff_status"].append(jira)
                    elif pivot_info["analysis_run_status"] == "complete":
                        duplicate_reasons["diff_status"].append(analysis[0])
                        duplicate_reasons_jira["diff_status"].append(analysis[1])
                elif version_compare == "smaller" and \
                analysis[4] == pivot_info["analysis_run_status"] and \
                analysis[5] == pivot_info["aligner"] and \
                analysis[6] == pivot_info["number_of_lanes"]:
                    duplicate_reasons["pipeline_downgrade"].append(id)
                    duplicate_reasons_jira["pipeline_downgrade"].append(jira)
                else:
                    duplicate_reasons["others"].append(id)
                    duplicate_reasons_jira["others"].append(jira)
                id = analysis[0]
                jira = analysis[1]
                pivot_info["pipeline_version"] = analysis[2]
                pivot_info["analysis_submission_date"] = analysis[3]
                pivot_info["analysis_run_status"] = analysis[4]
                pivot_info["aligner"] = analysis[5]
                pivot_info["number_of_lanes"] = analysis[6]


def delete_colossus(problem_ids):
    for problem_id in problem_ids:
        colossus_api.delete("analysis_information", id = problem_id)

def delete_tantalus(problem_jira_tickets):
    problem_id = []
    for ticket in problem_jira_tickets:
        ana_list = tantalus_api.list('analysis', jira_ticket = ticket)
        for ana in list(ana_list):
            problem_id.append(ana["id"])
    storage = "singlecellblob"
    for pid in problem_id:
        command = f"python server_cleanup.py delete-analyses {storage} -d {storage} -id {pid} --clean-azure"
        os.system(command)

def delete_jira(problem_jira_tickets):
    for ticket in problem_jira_tickets:
        try:
            delete_ticket(ticket)
        except:
            pass

def delete_dup_analysis(reason):
    print("-------START DELETION------------")
    delete_alhena(duplicate_reasons_jira[reason])
    print("-------FINISH ALHENA DELETION------------")
    delete_tantalus(duplicate_reasons_jira[reason])
    print("-------FINISH TANTALUS DELETION------------")
    delete_colossus(duplicate_reasons[reason])
    print("-------FINISH COLOSSUS DELETION------------")
    delete_jira(duplicate_reasons_jira[reason])
    print("-------FINISH JIRA DELETION------------")

def delete_alhena(problem_jira_tickets):
    try:
        print(problem_jira_tickets)
        start_vm("bccrc-pr-loader-vm", "bccrc-pr-cc-alhena-rg")
        host = "10.1.0.8"
        command_to_run = (
            "source /home/spectrum/alhena_bccrc/venv/bin/activate && source /home/spectrum/alhena-loader/set_credentials"
        )
        for ticket in problem_jira_tickets:
            command_to_run = command_to_run + f" && alhena_bccrc --host {host} --id {ticket} clean"
        ssh_command = ['ssh', 'loader', command_to_run]
        result = subprocess.run(ssh_command, capture_output=True, text=True)
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"Error: {result.stderr}")
        stop_vm("bccrc-pr-loader-vm","bccrc-pr-cc-alhena-rg")
    except Exception as e:
        print(f"{e}")

def temp_print():
    with open('other.csv', 'w+', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["lib_id", "analysis_id", "analysis_jira_ticket", "pipeline_version", "analysis_submission_date", "analysis_run_status", "aligner", "number_of_lanes"])
        writer.writerow(["", "", "", "", "", "", "", "", ""])
        lane = duplicate_reasons["lane_update"]
        for key, value in lib_ana_dict_duplicate.items():
            lib_id = key
            flag_lib = 0
            for analysis in value:
                if analysis[0] in lane:
                    flag_lib = 1
                    break
            if flag_lib == 0:
                for analysis in value:
                    analysis_id = analysis[0]
                    analysis_jira_ticket = analysis[1]
                    pipeline_version = analysis[2]
                    analysis_submission_date = analysis[3]
                    analysis_run_status = analysis[4]
                    aligner = analysis[5]
                    number_of_lanes = analysis[6]
                    writer.writerow([lib_id, analysis_id, analysis_jira_ticket, pipeline_version, analysis_submission_date, analysis_run_status, aligner, number_of_lanes])
                writer.writerow(["", "", "", "", "", "", "", "", ""])



    

@click.command()
@click.option("--pt", is_flag = True, default = False)
@click.option("--bug", is_flag = True, default = False) 
@click.option("--pipeline_u", is_flag = True, default = False) 
@click.option("--lane", is_flag = True, default = False) 
@click.option("--aligner", is_flag = True, default = False) 
@click.option("--pipeline_d", is_flag = True, default = False) 
@click.option("--status", is_flag = True, default = False) 
@click.argument("id", nargs=1)
@click.argument("ticket", nargs=1)
def main(pt, bug, pipeline_u, lane, aligner, pipeline_d, status, id=None, ticket=None):
    if id is not None and ticket is not None:
        print(f"Deleting analysis with id: {id} and jira ticket: {ticket}")
        duplicate_reasons["others"].append(id)
        duplicate_reasons_jira["others"].append(ticket)
        delete_dup_analysis("others")
        #print(duplicate_reasons)
        #print(duplicate_reasons_jira)
        return
    
    list_dup_information()
    identify_dup_reason()
    if pt:
        make_lib_ana_duplicate_csv()
        print(duplicate_reasons)
        print(duplicate_reasons_jira)
    if bug:
        delete_dup_analysis("bug")
    if pipeline_u:
        delete_dup_analysis("pipeline_update")
    if lane:
        delete_dup_analysis("lane_update")
    if aligner:
        delete_dup_analysis("aligner")
    if pipeline_d:
        delete_dup_analysis("pipeline_downgrade")
    if status:
        delete_dup_analysis("diff_status")
    

    

if __name__ == "__main__":
    main()