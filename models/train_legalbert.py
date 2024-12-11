import torch  
import os
import pandas as pd
import spacy
from spacy.tokens import DocBin
from simpletransformers.classification import ClassificationModel, ClassificationArgs
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
transformers_logger = logging.getLogger("transformers")
transformers_logger.setLevel(logging.WARNING)


def train_classification_model():
    train_data = [
        ["sentenza n. 1902/10 del Tribunale di Pisa", 1],
        ["cfr.cass.civ.sez.III 15.7.05 n.15019", 1],
        [" ex art.32 cost", 1],
        ["Un'erdita' s'intende devoluta, quando alcuno la puo' conseguire", 0],
        ["Ha un'apparenza, ma tutt'altra sostanza", 0],
        ["Il creditore no tenne conto delle cose nella loro individualita', ma bensì della quantita'", 0]
    ]
    train_df = pd.DataFrame(train_data, columns=["text", "labels"])

    val_data = [
        ["Suprema Corte, con sentenza del 15.5.2012, n. 7531", 1],
        ["Sentenza 11 febbraio 2015 n. 11", 1],
        ["Un diritto non ancora esercitabile non e' soggetto a prescrizione", 0],
        ["Chiunque puo' stipulare a favore d'un terzo", 0]
    ]
    val_df = pd.DataFrame(val_data, columns=["text", "labels"])

    use_cuda = torch.cuda.is_available()

    model_args = ClassificationArgs(
        num_train_epochs=4,
        evaluate_during_training=True,
        overwrite_output_dir=True
    )

    model = ClassificationModel(
        "bert",
        "dlicari/Italian-Legal-BERT",
        args=model_args,
        use_cuda=use_cuda  
    )

    model.train_model(train_df, eval_df=val_df)

    model.save_model("classification_model")

    print("Classification model trained and saved.")


def convert_to_spacy(ner_sample, file_to_save):
    nlp = spacy.blank("it")
    doc_bin = DocBin()
    for text, annotations in ner_sample:
        doc = nlp.make_doc(text)
        ents = []
        for start, end, label in annotations:
            span = doc.char_span(start, end, label=label,
                                 alignment_mode="contract")
            if span is not None:
                ents.append(span)
        doc.ents = ents
        doc_bin.add(doc)
    doc_bin.to_disk(file_to_save)


def train_ner_model():
    train_data = [
    (
        "L'articolo 13 del decreto legislativo 31 marzo 1998, n. 112, stabilisce le competenze",
        [(11, 13, 'ART'), (17, 52, 'DOC')]
    ),
    (
        "Come indicato all'articolo 5 della legge 7 agosto 1990, n. 241, le amministrazioni pubbliche...",
        [(27, 28, 'ART'), (35, 52, 'DOC')]
    ),
    (
        "In base all'articolo 15 del Codice civile si riconosce il diritto di successione...",
        [(21, 23, 'ART'), (28, 41, 'DOC')]
    ),
    (
        "L'articolo 6 della Costituzione tutela le minoranze linguistiche",
        [(11, 12, 'ART'), (18, 30, 'DOC')]
    ),
    (
        "Art 6 tuir",
        [(5, 6, 'ART'), (8, 11, 'DOC')]
    ),
    (
        "ex art 15, Codice della strada.",
        [(7, 9, 'ART'), (11, 30, 'DOC')]
    )
    ]



    
    convert_to_spacy(train_data, "train_data.spacy")

    
    os.system("python -m spacy train ner_it_legalbert.cfg --output ner_model --paths.train train_data.spacy --paths.dev train_data.spacy")

    print("NER model trained and saved.")


if __name__ == "__main__":
    train_classification_model()
    train_ner_model()
