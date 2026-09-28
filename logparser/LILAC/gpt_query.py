import openai
import os
import re
import time
import string
import json
from .parsing_cache import ParsingCache
from .post_process import correct_single_template


def get_openai_key():
    api_base = os.getenv('OPENAI_API_BASE', 'http://localhost:11434/v1')
    key_str = os.getenv('OPENAI_API_KEY', 'dummy')
    if not key_str and os.path.exists('../../openai_key.txt'):
        try:
            with open('../../openai_key.txt', 'r') as file:
                api_base = file.readline().strip()
                key_str = file.readline().strip()
        except Exception:
            pass
    if "11434" in api_base and not api_base.endswith("/v1"):
        api_base = api_base.rstrip("/") + "/v1"
    return api_base, key_str

import requests

def get_ollama_base():
    api_base = os.getenv('OLLAMA_API_BASE', 'http://localhost:11434')
    if "v1" in api_base:
        api_base = api_base.replace("/v1", "")
    if api_base.endswith("/"):
        api_base = api_base[:-1]
    return api_base

api_base = get_ollama_base()
print("LILAC LLM API Base:", api_base)

def infer_llm(instruction, exemplars, query, log_message, model='qwen2.5:7b', temperature=0.0, max_tokens=2048):
    # Dùng system prompt mạnh để cấm tạo block <think>
    instruction = instruction + "\nDO NOT use <think> tags. Output the answer IMMEDIATELY."
    
    messages = [{"role": "system", "content": "You are an expert of log parsing. DO NOT use <think> reasoning blocks. Give answers directly."},
                {"role": "user", "content": instruction},
                {"role": "assistant", "content": "Sure, I can help you with log parsing."},
                ]

    if exemplars is not None:
        for i, exemplar in enumerate(exemplars):
            messages.append({"role": "user", "content": exemplar['query']})
            messages.append({"role": "assistant", "content": exemplar['answer']})
    messages.append({"role": "user", "content": query})

    retry_times = 0
    print("model: ", model)
    url = f"{api_base}/api/chat"
    
    while retry_times < 3:
        try:
            payload = {
                "model": model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens
                }
            }
            # Attempt to pass flag if supported
            payload["options"]["think"] = False
            
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()
            data = response.json()
            
            content = data['message']['content']
            
            # Post-process: strip <think> blocks if Ollama still generates them
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            
            # Nếu nó trả về phần thừa, cắt từ chuỗi Log template
            return content
            
        except Exception as e:
            print("Exception :", e)
            retry_times += 1
            
    print(f"Failed to get response from Ollama after {retry_times} retries.")
    return 'Log message: `{}`'.format(log_message)


def get_response_from_openai_key(query, examples=[], model='gpt-3.5-turbo-0613', temperature=0.0):
    # Prompt-1
    instruction = "I want you to act like an expert of log parsing. I will give you a log message delimited by backticks. You must identify and abstract ALL dynamic variables (timestamps, IP addresses, numbers, etc) by replacing them with the exact string `{VAR}`. CRITICAL: DO NOT add, remove, or modify any static words, punctuation, or spaces from the original message. ONLY replace variables with `{VAR}`. Output a static log template. Print the input log's template delimited by backticks."
    if examples is None or len(examples) == 0:
        examples = [
            {'query': 'Log message: `open through proxy proxy.cse.cuhk.edu.hk:5070 HTTPS`', 'answer': 'Log template: `open through proxy {VAR} HTTPS`'},
            {'query': 'Log message: `18846 bytes (18.4 KB) received, lifetime <1 sec`', 'answer': 'Log template: `{VAR} bytes ({VAR}) received, lifetime <{VAR} sec`'},
            {'query': 'Log message: `PacketResponder 1 for block blk_2 terminating`', 'answer': 'Log template: `PacketResponder {VAR} for block {VAR} terminating`'}
        ]
    question = 'Log message: `{}`'.format(query)
    responses = infer_llm(instruction, examples, question, query,
                          model, temperature, max_tokens=2048)
    return responses


def query_template_from_gpt(log_message, examples=[], model='gpt-3.5-turbo-0613'):
    if len(log_message.split()) == 1:
        return log_message, False
    # print("prompt base: ", prompt_base)
    response = get_response_from_openai_key(log_message, examples, model)
    # print(response)
    lines = response.split('\n')
    log_template = None
    for line in lines:
        if line.find("Log template:") != -1:
            log_template = line
            break
    if log_template is None:
        for line in lines:
            if line.find("`") != -1:
                log_template = line
                break
    if log_template is not None:
        start_index = log_template.find('`') + 1
        end_index = log_template.rfind('`')

        if start_index == 0 or end_index == -1:
            start_index = log_template.find('"') + 1
            end_index = log_template.rfind('"')

        if start_index != 0 and end_index != -1 and start_index < end_index:
            template = log_template[start_index:end_index]
            return template, True

    print("======================================")
    print("ChatGPT response format error: ")
    print(response)
    print("======================================")
    return log_message, False


def post_process_template(template, regs_common):
    pattern = r'\{(\w+)\}'
    template = re.sub(pattern, "<*>", template)
    for reg in regs_common:
        template = reg.sub("<*>", template)
    template = correct_single_template(template)
    static_part = template.replace("<*>", "")
    punc = string.punctuation
    for s in static_part:
        if s != ' ' and s not in punc:
            return template, True
    print("Get a too general template. Error.")
    return "", False


def query_template_from_gpt_with_check(log_message, regs_common=[], examples=[], model="gpt-3.5-turbo-0613"):
    template, flag = query_template_from_gpt(log_message, examples, model)
    if len(template) == 0 or flag == False:
        print(f"ChatGPT error")
    else:
        tree = ParsingCache()
        template, flag = post_process_template(template, regs_common)
        if flag:
            tree.add_templates(template)
            if tree.match_event(log_message)[0] == "NoMatch":
                print("==========================================================")
                print(log_message)
                print("ChatGPT template wrong: cannot match itself! And the wrong template is : ")
                print(template)
                print("==========================================================")
            else:
                return template, True
    return post_process_template(log_message, regs_common)
