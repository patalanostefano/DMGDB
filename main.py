import re
import html
import argparse
import yaml
import logging
import json
import os
from datetime import datetime
from models.graph_att import GraphSearcher
from typing import Dict, Any
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_document_names(docs_folder):
    docs_path = os.path.join(os.path.dirname(__file__), docs_folder)
    return [f for f in os.listdir(docs_path) if os.path.isfile(os.path.join(docs_path, f))]


def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def save_result(result: Dict[str, Any], output_file: str):
    with open(output_file, 'a') as f:
        json.dump(result, f)
        f.write('\n')


def generate_response(prompt: str, bedrock_runtime, model_id: str = 'anthropic.claude-3-5-sonnet-20240620-v1:0', max_tokens: int = 1000):
    system_prompt = """sei un assistente RAG per uno studio legale autorizzato dai clienti grazie a una liberatoria a trovare informazioni e trattare dati personali rispondendo alle domande degli avvocati aiutandoli nel loro lavoro. In particolare hai a disposizione delle funzioni che puoi utilizzare per ricercare all'interno del database di documenti i contesti necessari per rispondere alla domanda dell'utente tramite una catena di ragionamento che verrà effettuata inserendo una struttura simile a un formato HTML ad ogni step. Sta a te capire in quale parte del ragionamento ti trovi in particolare:

<api>search_by_embedding('document','text')</api> //per cercare per similarità rispetto a text all'interno di un documento document
<api>search_by_category('document','category')</api> //per cercare entità di una specifica categoria all'interno di un documento 
//le categorie sono rispettivamente: ['articolo', 'azienda', 'organizzazione', 'località', 'soggetto', 'ruolo', 'ente giuridico', 'procedura legale', 'persona', 'indirizzo', 'data', 'importo', 'contratto', 'oggetto']
<api>search_by_text('document','text')</api> // per cercare per contenuto testuale uguale a text all'interno di un documento document
//ricerca nei testi giuridici:
<api>search_article('number','law')</api> //per cercare un articolo legale del diritto italiano all'interno di law (nome del documento es. codice civile)
<api>wide_article('text','law')</api> //per cercare un articolo legale del diritto italiano all'interno di law per similarità rispetto a text
// Queste cinque API di ricerca possono essere chiamate più volte nella stessa risposta.
// L'argomento 'document' nelle chiamate delle API è opzionale. Se 'document' non viene specificato, la ricerca verrà eseguita su tutti i documenti presenti nel grafo.

<api>end_querying(response)</api> // con questo se avrai terminato definitivamente e sei capace di rispondere alla domanda inserisci la risposta al posto di response e concludi (nella risposta includi le fonti se presenti nel contesto (con [Fonte: nome documento])

Compito: il tuo compito è rispondere senza includere altro testo solo chiamando una o più delle <api> sopra descritte nello stesso formato <api>funzione('document','argomento')</api>, (possono essere chiamate anche più volte nella stessa risposta), come indicato di seguito:
ogni volta nel (<corpo>) riceverai la domanda (<domanda>) e/o i (<contesti>) e prima di ogni contesto trovato (all'interno di <contesti></contesti>) troverai la chiamata API che hai effettuato precedentemente in modo che tu possa variare (cioè quella che ti ha prodotto tale contesto) all'interno di (<from></from>); e soprattutto sempre <iterazione> che devi seguire per conoscere il numero massimo di iterazioni di ricerca che puoi effettuare prima di restituire la risposta finale come indicato di seguito.
rispetto alla domanda (<domanda>) dell'utente dovrai scegliere quali API chiamare e:
Una volta che riceverai la risposta tramite (<contesti trovati>) puoi utilizzare di nuovo le API di ricerca utilizzando alcuni contenuti dei contesti trovati per approfondire i dettagli per rispondere alla domanda solo se l'iterazione corrente è minore o uguale a 4 (cioè hai: <iterazione>4</iterazione>) OPPURE puoi terminare (in qualsiasi momento ma DEVI FARLO necessariamente se <iterazione>5</iterazione>, IN TAL CASO RISPONDI ALLA DOMANDA sulla base dei contesti raccolti fino a quel momento) chiamando <api>end_querying</api> restituendo la risposta.
attenzione: spesso i documenti contengono informazioni sovrapposte cioè riguardanti le stesse persone o le stesse transazioni o importi, attenzione a non considerare informazioni che riguardano stessi elementi in documenti diversi come elementi diversi.
non includere nella risposta altro testo che non sia nel formato <api></api>

    """

    messages = [
        {"role": "user", "content": prompt}
    ]

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": messages
    })

    try:
        response = bedrock_runtime.invoke_model(body=body, modelId=model_id)
        response_body = json.loads(response.get('body').read())

        if 'content' in response_body:
            return response_body['content'][0]['text'].strip()
        else:
            logger.error(f"Unexpected response structure: {response_body}")
            return "Error: Unable to generate response"
    except Exception as e:
        logger.error(f"Error in generate_response: {str(e)}")
        return f"Error: {str(e)}"


def parse_api_calls(response):
    api_calls = re.findall(r'<api>(.*?)</api>', response, re.DOTALL)

    if not api_calls:
        print("Error: No valid API call found in the response.")
        return []

    parsed_calls = []
    for api_call in api_calls:
        if api_call.strip().startswith('end_querying'):
            final_response = re.search(
                r'end_querying\((.*)\)', api_call, re.DOTALL)
            if final_response:
                final_answer = html.unescape(final_response.group(1).strip())
                parsed_calls.append(('end_querying', final_answer))
        elif api_call.strip().startswith(('search_by_embedding', 'search_by_text', 'search_by_category', 'search_article','wide_search')):
            func_name, args = api_call.strip().split('(', 1)
            args = args.rstrip(')').split(',', 1)
            document = args[0].strip("'")
            text = args[1].strip("'") if len(args) > 1 else ""
            parsed_calls.append((func_name, document, text))

    return parsed_calls


def main(config: Dict[str, Any]):
    searcher = GraphSearcher(config['neo4j_config'])

    bedrock_runtime = boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1"
    )

    results_folder = os.path.join(os.path.dirname(__file__), "Results")
    os.makedirs(results_folder, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(results_folder, f"results_{timestamp}.jsonl")

    print("\nLexyGraphAI")

    doc_names = get_document_names('data/docs')
    doc_names_str = ', '.join(f"'{doc}'" for doc in doc_names)

    base_prompt = """
<corpo>
<domanda>{question}</domanda>
<iterazione>{iteration_count}</iterazione>
<contesti>{contexts}</contesti>
</corpo>
inoltre : hai a disposizione la lista dei nomi dei documenti: [{doc_names_str}] dovrai usare tali nomi nelle api chiamate.
"""

    while True:
        question = input("\nQ&Aagent: ").strip()
        if question.lower() == 'exit':
            break

        contexts = []  
        model_responses = []
        iteration_count = 0
        max_iterations = 10

        while iteration_count < max_iterations:
            iteration_count += 1

            # Prepare the prompt
            contexts_str = "\n".join(
                [f"<from>{api_call}</from>\n{context}" for api_call, context in contexts])
            prompt = base_prompt.format(
                question=question, iteration_count=iteration_count, contexts=contexts_str, doc_names_str=doc_names_str)
            print(f"Iteration {iteration_count} prompt:\n{prompt}")

            # Generate response using Bedrock
            response = generate_response(prompt, bedrock_runtime)
            model_responses.append(response)
            print(f"Model response:\n{response}")

            # Parse the API calls using the new function
            parsed_api_calls = parse_api_calls(response)

            if not parsed_api_calls:
                print("Error: No valid API call found in the response.")
                break

            new_contexts = []
            for api_call in parsed_api_calls:
                if api_call[0] == 'end_querying':
                    final_answer = api_call[1]
                    print("\nFinal Answer:")
                    print(final_answer)

                    # Save result
                    result = {
                        'question': question,
                        'model_responses': model_responses,
                        'final_answer': final_answer
                    }
                    save_result(result, output_file)
                    break
                else:
                    func_name, document, text = api_call
                    print(
                        f"Calling {func_name} with document: {document}, text: {text}")

                    # Call the appropriate function
                    try:
                        if func_name == 'search_by_embedding':
                            results = searcher.search_by_embedding(
                                document, text, config['limit'])
                        elif func_name == 'search_by_text':
                            results = searcher.search_by_text(
                                document, text, config['limit'])
                        elif func_name == 'search_by_category':
                            results = searcher.search_by_category(
                                document, text, config['limit'])
                        elif func_name == 'search_article':
                            results = searcher.search_by_article(
                                document, text)
                        elif func_name == 'wide_search':
                            results = searcher.wide_search(
                                document, text)

                        # Append results to new_contexts
                        new_contexts.extend(
                            [(f"{func_name}('{document}','{text}')", result) for result in results])

                        print(f"Retrieved {len(results)} results")
                    except Exception as e:
                        print(f"Error in {func_name}: {str(e)}")

            if api_call[0] == 'end_querying':
                break

            # Update contexts with new results
            combined_chunks = searcher.combine_chunks(
                [context for _, context in new_contexts])
            contexts.extend([(api_call, chunk) for (api_call, _),
                            chunk in zip(new_contexts, combined_chunks)])

            if iteration_count >= max_iterations:
                print("Maximum number of iterations reached without a final answer.")
                break

    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True,
                        help='Path to config file')
    args = parser.parse_args()

    config = load_config(args.config)
    main(config)
