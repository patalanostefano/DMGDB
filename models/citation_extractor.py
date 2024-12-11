import spacy
from transformers import BertTokenizer, BertForSequenceClassification
import torch

class LegalCitationPipeline:
    def __init__(self):
        self.tokenizer = BertTokenizer.from_pretrained("models/outputs/")
        self.classification_model = BertForSequenceClassification.from_pretrained("models/outputs/")
        
        model_path = "models/ner_model/model-last"
        self.ner_model = spacy.load(model_path)
        
        self.max_length = 512

    def classify_text(self, text):
        encoded = self.tokenizer.encode_plus(
            text,
            max_length=self.max_length,
            truncation=True,
            padding='max_length',
            return_tensors="pt"
        )
        
        try:
            with torch.no_grad():
                outputs = self.classification_model(**encoded)
                logits = outputs.logits
                predicted_class = logits.argmax(dim=-1).item()
            return predicted_class
        except Exception as e:
            print(f"Error in classification: {e}")
            return 0  

    def process_text(self, text):
        try:
            doc = self.ner_model(text)

            citations = []
            current_doc = None
            
            entities = list(doc.ents)
            
            for i, ent in enumerate(entities):
                if ent.label_ == "DOC":
                    current_doc = ent.text
                elif ent.label_ == "ART":
                    if current_doc:
                        citations.append((current_doc, ent.text))
                    else:
                        for next_ent in entities[i+1:]:
                            if next_ent.label_ == "DOC":
                                citations.append((next_ent.text, ent.text))
                                break

            return citations
            
        except Exception as e:
            print(f"Error processing text: {e}")
            return []

    def process_batch(self, texts):
        results = []
        for text in texts:
            if text:  
                try:
                    result = self.process_text(text)
                    results.append(result)
                except Exception as e:
                    print(f"Error processing batch item: {e}")
                    results.append([])
            else:
                results.append([])
        return results