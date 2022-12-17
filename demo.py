#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 13 18:26:47 2022

@author: ziyanghu
"""

#libraries
## data reading
import pandas as pd
import os
import pprint
## Stanford CoreNLP
from pycorenlp import StanfordCoreNLP
import subprocess
import time
import json
## stanza
import stanza
## CUI search
import requests

## CUI sort
import re

## db export
import sqlite3
#functions

def triple_mining(CoreNLP_output, stanza_output, CUIs_dict):
    """
    This function collects and filters the relation triples extracted by Stanford CoreNLP 
    with reference from biomedical entities extracted from the text by Stanza
    
    
    Relation triples: subject-relation-object / head-relation-tail/subject-predicate-object
    CUI: concept unique identifier; reference: https://www.nlm.nih.gov/research/umls/new_users/online_learning/Meta_005.html
    """
    entities_set = set()#the set of intities
    sentence_id = 1 # as a reference to resolve pronominal reference
    for entity_dict in stanza_output.entities:#collect unique entities from Stanza output
        entity_text = entity_dict.text
        entities_set.add(entity_text.lower())
    entities_list = list(entities_set) #the list of unique medical entities
    
    triple_list = []#store the extracted relation triples
    
    
    for sent_dict in CoreNLP_output['sentences']:#query each sentence in the output of CoreNLP
        print("                Processing sentence", sentence_id)
        openie_list = sent_dict["openie"]
        for openie_dict in openie_list:#query extracted triples
            # if the elements in the extracted triple is pronominal reference
            # it will subsititued by what it refered to in the text based on Coreference data in CoreNLP output
            subj = coreference_resolution(sentence_id, 
                                          openie_dict["subjectSpan"][0],
                                          openie_dict["subjectSpan"][1],
                                          openie_dict["subject"],
                                          CoreNLP_output['corefs'])
            obj = coreference_resolution(sentence_id, 
                                          openie_dict["objectSpan"][0],
                                          openie_dict["objectSpan"][1],
                                          openie_dict["object"],
                                          CoreNLP_output['corefs'])
            predicate = coreference_resolution(sentence_id, 
                                          openie_dict["relationSpan"][0],
                                          openie_dict["relationSpan"][1],
                                          openie_dict["relation"],
                                          CoreNLP_output['corefs'])

            for entity in entities_list:
                #a triple will be included in the output if at least 1 element contains extracted medical entity
                if (entity in subj.lower()) or (entity in obj.lower())  or (entity in predicate.lower()):
                    #for each element in the included triple, if it is a medical entity, it's relevant CUIs will be
                    #searched and stored as a list in the CUIs dictionary with element in lower case as the key
                    try:
                        subj_CUIs_list = CUIs_dict[subj.lower()]
                    except:
                        if (entity in subj.lower()):
                            subj_CUIs_list = CUIs_list(subj)
                            CUIs_dict[subj.lower()] = subj_CUIs_list
                        else:
                            subj_CUIs_list = []
                            
                    try:
                        predicate_CUIs_list = CUIs_dict[predicate.lower()]
                    except:
                        if (entity in predicate.lower()):
                            predicate_CUIs_list = CUIs_list(predicate)
                            CUIs_dict[predicate.lower()] = predicate_CUIs_list
                        else:
                            predicate_CUIs_list = []
                    
                    try:
                        obj_CUIs_list = CUIs_dict[obj.lower()]
                    except:
                        if (entity in obj.lower()):
                            obj_CUIs_list = CUIs_list(obj)
                            CUIs_dict[obj.lower()] = obj_CUIs_list
                        else:
                            obj_CUIs_list = []
                    #store the extracted triple information
                    triple_list.append({"subject": subj,
                                        "subject CUIs": subj_CUIs_list,
                                        "relation": predicate,
                                        "relation CUIs": predicate_CUIs_list,
                                        "object": obj,
                                        "object CUIs": obj_CUIs_list
                                        })
                    break
        sentence_id += 1
    
    #the updated CUIs dictionary is also returned
    return triple_list, CUIs_dict




def coreference_resolution(sentence_id, start_id, end_id, element_text, coref_data):
    """
    This function checks if a part of the sentence is a pronomial coreference. If it is
    and the first reference in text is nominal, the first reference will be returned,
    otherwise, the original term will be returned
    """
    #check each unique reference
    for coref in coref_data:
          mentions = coref_data[coref]
          antecedent = mentions[0]#the first reference
          #if the first reference is also pronominal, the original text will be returned
          if antecedent["type"] == "NOMINAL":
            for j in range(1, len(mentions)):
                mention = mentions[j]
                #check if the input term is a reference by matching the sentence index, start index, and end index
                if mention["sentNum"] == sentence_id:
                    if mention["startIndex"] == start_id and mention["endIndex"] == end_id:
                       
                        return antecedent["text"]
    return element_text


def CUIs_list(string, 
                 apikey = "6dc74465-0dee-4d66-9351-ace84f50e51e",
                 version = "current"):
    """
    This function returns a list of sorted CUIs that is relevant to the input string.
    
    It is based on search-terms.py from UMLS Python scripts: 
        https://documentation.uts.nlm.nih.gov/rest/rest-api-cookbook/python-scripts.html
    """
    
    #sorting functions
    ## source: https://codereview.stackexchange.com/questions/274824/sort-a-python-list-of-strings-where-each-item-is-made-with-letters-and-numbers
    COMPILED = re.compile(r"([A-Z]+)([0-9]+)", re.I)

    def sort_key(item: str) -> tuple[str, int]:
        match = COMPILED.match(item)
        return match[1], int(match[2])
    
    #query and search
    uri = "https://uts-ws.nlm.nih.gov"
    content_endpoint = "/rest/search/"+version
    full_url = uri+content_endpoint
    page = 0
    CUIs = []
    try:
        
        while True:
            page += 1
            query = {'string':string,'apiKey':apikey, 'pageNumber':page}
            r = requests.get(full_url,params=query)
            r.raise_for_status()
            r.encoding = 'utf-8'
            outputs  = r.json()
        
            items = (([outputs['result']])[0])['results']
            
            if len(items) == 0:
                if page == 1:
                    break
                else:
                    break
            
            
            for result in items:
                CUIs.append(result['ui'])
                
        return sorted(CUIs, key=sort_key)
        # return(CUIs)

    except Exception as except_error:
        return([str(except_error)])
    

    
def CUIs_to_str(CUIs):
    """
    This function turn a list of CUIs into a string
    """
    if len(CUIs) == 0:#empty list
        return ""
    if len(CUIs) == 1:#list with 1 element
        return CUIs[0]
    else:
        return("|".join(CUIs))# '|' between each CUI as separator



def ID_assign(identifier,idx, id_dict):
    """
    This function assign a sequence ID to a given identifier,
    it also updates the ID dictionary and the new largeat ID
    """
    try: #if already exist in dictionary, assign old id
        id_assign = id_dict[identifier]
    except:
        id_assign = idx + 1 #new id
        idx = id_assign #update new largeat ID (index)
        id_dict[identifier] = id_assign #update dictioary
        
    return id_assign, idx, id_dict


def relational_tables_generation(triple_list):
    """
    This function generates relational data: 
        a table of relation labels
        a table of entity labels
    """
    rel_id_dict = {}
    ent_id_dict = {}
    rel_id = 0
    ent_id = 0
    relation_list = []
    relation_label_dict = {}
    entity_label_dict = {}
    # triple_back_up = triple_list

    for triple_dict in triple_list:
        
        rel_CUI_str = CUIs_to_str(triple_dict['relation CUIs'])
        if len(rel_CUI_str) == 0:
            rel_id_assign, rel_id, relation_id_dict = ID_assign(triple_dict["relation"], rel_id, rel_id_dict)    
        else:
            rel_id_assign, rel_id, relation_id_dict = ID_assign(rel_CUI_str, rel_id, rel_id_dict)    
        
        # triple_dict["relation uid"] = rel_id_assign
        
        
        subj_CUI_str = CUIs_to_str(triple_dict['subject CUIs'])
        obj_CUI_str = CUIs_to_str(triple_dict['object CUIs'])
        

        if len(subj_CUI_str) == 0:
            subj_id_assign, ent_id, ent_id_dict = ID_assign(triple_dict["subject"], ent_id, ent_id_dict)    
        else:
            subj_id_assign, ent_id, ent_id_dict = ID_assign(subj_CUI_str, ent_id, ent_id_dict)  
        
        # triple_dict["subject uid"] = subj_id_assign
            
        if len(obj_CUI_str) == 0:
            obj_id_assign, ent_id, ent_id_dict = ID_assign(triple_dict["object"], ent_id, ent_id_dict)    
        else:
            obj_id_assign, ent_id, ent_id_dict = ID_assign(obj_CUI_str, ent_id, ent_id_dict)  
                                      
        # triple_dict["object uid"] = obj_id_assign
        relation_dict = {}
        relation_dict["subject_id"] =  subj_id_assign
        relation_dict["relation_id"] = rel_id_assign
        relation_dict["objcect_id"] = obj_id_assign
        relation_list.append(relation_dict)
        
        if rel_id_assign not in relation_label_dict.keys():
            relation_label_dict[rel_id_assign] = {"CUIs":rel_CUI_str,
                                                  "text_reference":triple_dict["relation"]}
        
        if subj_id_assign not in entity_label_dict.keys():
            entity_label_dict[subj_id_assign] = {"CUIs":subj_CUI_str,
                                                  "text_reference":triple_dict["subject"]}
        if obj_id_assign not in entity_label_dict.keys():
            entity_label_dict[obj_id_assign] = {"CUIs":obj_CUI_str,
                                                  "text_reference":triple_dict["object"]}
        
    
    return relation_list, relation_label_dict, entity_label_dict

def dict_to_list(id_name,data_dict):
    """
    This function's input is a dictionary whose keys are unique ids, and values are dictionaries.
    This function add the key/unique id into its value (a dictionary) and make it into a list.
    """
    data_list = []
    for unique_id in data_dict.keys():
        val_dict = data_dict[unique_id]
        val_dict[id_name] = unique_id
        data_list.append(val_dict)
    return data_list
        

def db_export(output_dir, db_name, table_dict):
    """
    This function save the relational tables into a db file.
    the table_dict's keys are table names, and the values are lists of dictionaries
    """
    conn = sqlite3.connect(os.path.join(output_dir, db_name+".db")) 
    for table_name in table_dict.keys():
        df = pd.DataFrame.from_dict(table_dict[table_name])
        df.to_sql(name = table_name, 
                          con = conn,
                          if_exists = "replace",
                          index = False)
    conn.commit()
    
    
def transitive_closure(output_dir, db_name, rel_id):
    """
    This function computes the transitive closure table of a given relation id
    """
    conn = sqlite3.connect(os.path.join(output_dir, db_name+".db")) 
    cur = conn.cursor()
    result = []
    #query the relation table
    SPO = cur.execute("SELECT subject_ID, object_ID FROM Edge WHERE relation_ID == "+str(rel_id)+";").fetchall()
    print(len(SPO))
    for spo_tuple in SPO:
        result.append([spo_tuple[0], spo_tuple[1], 1])
    print(len(result))
    direct_connect = result
    pprint.pprint(direct_connect[0:20])
    l = 2
    while True:
        print("For path length of:", l)
        last_path = []
        for entry in result:
            if entry[2] == l - 1:
                print("append edge to last entry: ", entry)
                last_path.append(entry)
        step_result = []
        for last_step_edge in last_path:
            sub = last_step_edge[0]
            obj = last_step_edge[1]
            for direct_edge in direct_connect:
                if direct_edge[1] == sub:
                    step_result.append([direct_edge[0], obj, l])
        l += 1
        if len(step_result) == 0:
            break
        else: 
            result.extend(step_result)
    return result
        
        
def distance_matrix(list_table, id_name):
    """
    This function computes the distance matrix and store it in a dictionary
    """
    result = {}
    for i in range(len(list_table)):
        data_dict = list_table[i]
        if not distance_computable(data_dict):
            continue
        id_val = data_dict[id_name]
        node_result = {}
        for j in range(i+1, len(list_table)):
            opposite_data_dict = list_table[j]
            if not distance_computable(opposite_data_dict):
                continue
            opposite_id = opposite_data_dict[id_name]
            node_result[opposite_id] = jaccard(set(data_dict["CUIs"].split('|')),
                                               set(opposite_data_dict["CUIs"].split('|')))
        if len(list(node_result.keys())) > 0:
            result[id_val] = node_result
    return result
        
def distance_computable(data_dict):
    """
    This function detect if a relation or entity can be used to compute distance matrix:
    associated with at least 2 CUIs
    """
    result = True
    if len(data_dict["CUIs"]) == 0 or 'error' in data_dict["CUIs"].lower():
        return False
    if len(data_dict["CUIs"].split('|')) == 1:
        return False
    return result


def jaccard(CUIs1, CUIs2):
    """
    This function computes jaccard index based on CUIs associated with two entites / relations
    each input is a set of CUIs
    """
    return len(CUIs1 & CUIs2) / len(CUIs1 | CUIs2)
    
    















#demo
work_dir = "/Users/ziyanghu/Desktop/ClaudeCase/Duke/Academic/2022_Fall/BIOSTAT_823/final"
#read dataset
##kaggle dataset: https://www.kaggle.com/datasets/andrewmvd/chronic-fatigue-syndrome-scientific-literature
csv_name = os.path.join(work_dir, "mecfs_dataset.csv")

df = pd.read_csv(csv_name, sep = "|")

abstracts = df["Abstract"]
id_text_dict = {}

#Stanford CoreNLP pipeline
memory_var = 4
document_length = 90000
sentence_length = 100
CoreNLPdir = "/Users/ziyanghu/Desktop/stanford-corenlp-4.5.1"
outputDir = "/Users/ziyanghu/Desktop/ClaudeCase/Duke/Academic/2022_Fall/BIOSTAT_823/final"
CoreNLP_nlp = subprocess.Popen(
                ['java', '-mx' + str(memory_var) + "g", '-cp', os.path.join(CoreNLPdir, '*'),
                 'edu.stanford.nlp.pipeline.StanfordCoreNLPServer',  '-parse.maxlen' + str(sentence_length), '-timeout', '999999'])
time.sleep(5)
nlp = StanfordCoreNLP('http://localhost:9000')

params = {'annotators':"ner, openie, coref",
               'parse.model': 'edu/stanford/nlp/models/parser/nndep/english_UD.gz',
               'outputFormat': 'json',
               'outputDirectory': outputDir,
               'replaceExtension': True,
               'parse.maxlen': str(sentence_length),
               'ner.maxlen': str(sentence_length),
               'pos.maxlen': str(sentence_length)}

#stanza pipeline
stanza.download('en', package='mimic')
med_nlp = stanza.Pipeline('en', package='mimic', processors={'ner': 'i2b2'})



#pipeline 1
batch_100 = df.head(100)
batch_triple_list = []
batch_CUIs_dict = {}

for index, row in batch_100.iterrows():
    pmc_id = row['ArticleId']
    abstract = row['Abstract']
    print("Start processing article:", pmc_id)
    # id_text_dict[row['ArticleId']] = row['Abstract']
    CoreNLP_output = nlp.annotate(abstract, properties=params)
    CoreNLP_output = json.loads(CoreNLP_output)
    stanza_output = med_nlp(abstract)
    article_result, CUIs_dict = triple_mining(CoreNLP_output, stanza_output, batch_CUIs_dict)
    batch_triple_list.extend(article_result)
    print("Finish processing article:", pmc_id)


#pipeline 2
relation_list, relation_label_dict, entity_label_dict = relational_tables_generation(batch_triple_list)
relation_label_list = dict_to_list("relation_id", relation_label_dict)
entity_label_list = dict_to_list("entity_id", entity_label_dict)

table_dict = {"Relation_Labels_Import": relation_label_list,
              "Entity_Labels_Import": entity_label_list,
              "Relation_Import": relation_list}

db_export(work_dir, "mecfs", table_dict)

relation_d_mat = distance_matrix(relation_label_list, "relation_id")
entity_d_mat = distance_matrix(entity_label_list, "entity_id")

CoreNLP_nlp.kill()

