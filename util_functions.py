#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 16:22:13 2022

@author: ziyanghu
"""

## Stanford CoreNLP

## CUI search
import requests

#functions
def triple_mining(CoreNLP_output, stanza_output, CUIs_dict):
    """
    This function collects and filters the triple relations extracted by Stanford CoreNLP 
    with reference from biomedical entities extracted from the text by Stanza
    
    
    Triple relations: subject-relation-object / head-relation-tail/subject-predicate-object
    CUI: concept unique identifier; reference: https://www.nlm.nih.gov/research/umls/new_users/online_learning/Meta_005.html
    """
    entities_set = set()#the set of intities
    sentence_id = 1 # as a reference to resolve pronominal reference
    for entity_dict in stanza_output.entities:#collect unique entities from Stanza output
        entity_text = entity_dict.text
        entities_set.add(entity_text.lower())
    entities_list = list(entities_set) #the list of unique medical entities
    
    triple_list = []#store the extracted triple relations
    
    
    for sent_dict in CoreNLP_output['sentences']:#query each sentence in the output of CoreNLP
        print("                Processing sentence", sentence_id)
        openie_list = sent_dict["openie"]
        for openie_dict in openie_list:#query extracted relations
            # if the elements in the extracted triple relation is pronominal reference
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
    This function returns a list of CUIs that is relevant to the input string.
    
    It is based on search-terms.py from UMLS Python scripts: 
        https://documentation.uts.nlm.nih.gov/rest/rest-api-cookbook/python-scripts.html
    """
    
    
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
                
        return CUIs

    except Exception as except_error:
        return([except_error])
    
    