from gliner import GLiNER


class EntityExtractor:
    _instance = None
    _model = None

    @staticmethod
    def get_instance():
        if EntityExtractor._instance is None:
            EntityExtractor._instance = EntityExtractor()
        return EntityExtractor._instance

    def __init__(self):
        if EntityExtractor._model is None:
            EntityExtractor._model = GLiNER.from_pretrained(
                "DeepMount00/GLiNER_PII_ITA")

    def extract_entities(self, text, labels=None):
        if labels is None:
            labels = [
                "azienda", "organizzazione", "località", "soggetto", "ruolo", "ente giuridico", "procedura legale", "persona", "indirizzo", "data", "numero", "importo", "contratto", "oggetto", "legge"]
        return EntityExtractor._model.predict_entities(text, labels)
