import os
import fitz  
import spacy
from spacy.language import Language
from models.entity_extractor import EntityExtractor
from typing import List, Dict
import re
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import torch
from transformers import AutoTokenizer, AutoModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModelEmbeddings:
    def __init__(self, legal: bool = True):
        self.legal = legal
        self.model_name = "dlicari/Italian-Legal-BERT" if legal else "DeepMount00/Anita"
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        logger.info(f"Loading model: {self.model_name} on {self.device}")
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name).to(self.device)
        self.embedding_size = self.model.config.hidden_size
        logger.info(f"Model loaded with embedding size: {self.embedding_size}")

    def mean_pooling(self, model_output, attention_mask):
        token_embeddings = model_output[0]
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
        sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
        return sum_embeddings / sum_mask

    def get_embeddings(self, texts: List[str], max_length: int = 512) -> List[List[float]]:
        try:
            encoded_input = self.tokenizer(
                texts,
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors='pt'
            ).to(self.device)

            with torch.no_grad():
                model_output = self.model(**encoded_input)

            embeddings = self.mean_pooling(
                model_output,
                encoded_input['attention_mask']
            )
            
            return embeddings.cpu().numpy().tolist()

        except Exception as e:
            logger.error(f"Error generating embeddings: {str(e)}")
            return [[0.0] * self.embedding_size] * len(texts)

    def embed_query(self, text: str) -> List[float]:
        return self.get_embeddings([text])[0]


class Embedder:
    def __init__(self, legal: bool = True):
        self.model_embedder = ModelEmbeddings(legal=legal)
        self.embedding_size = self.model_embedder.embedding_size
        logger.info(
            f"Embedder initialized with {'Legal' if legal else 'General'} model "
            f"and embedding size: {self.embedding_size}"
        )

    def get_embedding(self, text: str) -> List[float]:
        embedding = self.model_embedder.embed_query(text)
        if len(embedding) != self.embedding_size:
            logger.warning(
                f"Unexpected embedding size for text: {text[:50]}..."
            )
        return embedding

    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self.model_embedder.get_embeddings(texts)


class Chunker:
    _instance = None

    @staticmethod
    def get_instance():
        if Chunker._instance is None:
            Chunker._instance = Chunker()
        return Chunker._instance

    def __init__(self, max_chunk_size=72):
        self.max_chunk_size = max_chunk_size
        self.entity_extractor = EntityExtractor.get_instance()
        self.nlp = spacy.load('it_core_news_sm')
        self.embedder = Embedder()
        self.max_sentences_per_chunk = 2

        # Custom abbreviations for Italian

        self.abbreviations = set([
            'art', 'dr', 'dott', 'prof', 'ing', 'arch', 'avv', 'sig', 'st', 'ss',

            'v', 'vs', 'etc', 'ecc', 'es', 'pag', 'n', 'nr', 'fig', 'tab', 'ibid',

            'cap', 'cit', 'op', 'cf', 'ca', 'max', 'min', 'ab', 'ad', 'al', 'c', ':', 'S.p',

            'd', 'f', 'h', 'i', 'l', 'm', 'o', 'p', 'q', 'r', 's', 't', 'u', 'x', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '(', 'ac', 'ad', 'all', 'Amn', 'Arch', 'Avv', 'Bcc', 'CA', 'ca', 'CAP', 'CAP', 'Cc', 'c/c banc', 'c/c post', 'ccp', 'Chiamo', 'cm', 'c/o', 'Co', 'cp', 'CP', 'Cpr', 'Cso', 'corr', 'cs', 'cv', 'Dott', 'Dottssa', 'Dr', 'ecc', 'Egr', 'Egri', 'Egria', 'epc', 'fatt', 'Flli', 'Gentmo', 'Gentmi', 'Gentma', 'Gentme', 'Geom', 'g', 'gg', 'Id', 'Illmo', 'Ing', 'int', 'lett', 'Mo', 'Mons', 'NB', 'N/', 'ns', 'n°', 'n', 'ogg', 'on', 'p', 'pag', 'p', 'pp', 'pc', 'pc', 'pcc', 'pes', 'pf', 'pr', 'PS', 'pv', 'Pza', 'PT', 'Pregmo', 'Profssa', 'Prof', 'R', 'racc', 'Rag', 'Rev',

            'ric', 'Rif', 'RP', 'RSVP', 'SA', 'S acc', 'SBF', 'S E & O', 'seg', 'sgg', 'ss', 'Sig', 'Sigg', 'Signa', 'Sigra', 'Sigra/signa', 'snc', 'Soc', 'SpA', 'Spett', 'SPM', 'Srl', 'Stimmo', 'tel', 'us', 'V', 'V/', 'vs', 'Vip', 'Vle', 'VP', 'vr', 'vs', 'AAS', 'ABGB', 'abr', 'acc', 'al', 'all', 'art', 'ASS', 'Atti parl Cam', 'Atti parl Sen', 'Aue',

            'BUAbr', 'BUBasil', 'BUCal', 'BUCamp', 'BUEmRom', 'BUFVG', 'BULazio', 'BULig', 'BULomb', 'BUMarche', 'BUMol', 'BUPiem', 'BUPuglia', 'BUSar', 'BUTAA', 'BUTosc', 'BUUmbria', 'BUVdA', 'BUVen', 'Bull off', 'C', 'can', 'cap', 'cc', 'cceo', 'cc fr', 'c comm', 'c comm fr', 'CdM', 'Cedu', 'cic', 'circ', 'c nav', 'co', 'conc', 'conv', 'convenz', 'cost', 'cp', 'cpc', 'cpc fr', 'cp fr', 'cpmilg', 'cpmilp', 'cpp', 'cpp fr', 'cpv', 'D', 'dCG', 'dCpS', 'dec', 'dec Ceca', 'dec Cee', 'dec Ce', 'del', 'd interm', 'dir', 'dir Ceca', 'dir Cee', 'dir Ce', 'disp att', 'disp att coord', 'disp coord', 'disp prel', 'disp trans', 'dl', 'dlg', 'dlg CpS', 'dlg lt', 'dlg PC', 'dlg PR', 'dl lt', 'dlt', 'dm', 'doc', 'dPC', 'dPR', 'd ric amm', 'GU', 'GUCE', 'GUSic',

            'lin', 'l lav pa', 'l miner', 'l proc', 'l prov B', 'l prov T', 'l rg', 'l tar', 'm', 'mod', 'OR', 'ord giud', 'ord giur amm', 'prot', 'rd', 'rdl', 'rd lg', 'reg', 'reg ass benef', 'reg brev', 'reg Ceca', 'reg Cee', 'reg Ce', 'reg c nav', 'reg com prov', 'reg cont', 'reg esec', 'reg nav aerea', 'reg proc C conti', 'reg ps', 'reg rg', 'rel', 'st', 'tab', 'tit', 'tr', 'tr Ce', 'tr Ceca', 'tu', 'tu acque pubbl', 'tu avv St', 'tu b cult', 'tu C conti', 'tu com prov 1915', 'tu com prov 1934', 'tu Cons St', 'tu doc amm', 'tu edil', 'tu espr pu', 'tu fin loc', 'tu immigr', 'tu imp civ St', 'tu imp dr 1986', 'tu inf lav', 'tu pensioni', 'tu ps', 'vig', 'abr', 'arg', 'c', 'cap', 'cd', 'cfr', 'cit', 'conf', 'diff', 'ed', 'ined', 'lb', 'loc', 'lett', 'n', 'nt', 'op', 'p', 'pt', 'pt g', 'pt s', 'rist', 'rubr', 's ss', 'sd', 'sl', 'sess', 'sez', 'sr', 'suppl', 't', 'ult', 'v', 'vig', '§', 'App', 'App mil', 'Ass', 'Ass app', 'Cass', 'C conti', 'C cost', 'C eur du', 'C giust Ceca', 'C giust Ce', 'CIJ', 'Comm brevetti', 'Comm centr imp', 'Comm ed pop', 'Comm tribut 1o', 'Comm tribut 2o', 'Comm tribut centr', 'Cons reg sic', 'Cons St', 'CPJI', 'C reg sic', 'Gpa', 'Pret',

            'Tar', 'Trib', 'Trib acque', 'Trib Ce', 'Trib mil', 'Trib sup acque', 'Trib supr mil', 'D disc pen', 'D disc priv', 'D disc pubbl', 'DI', 'Enc Br', 'Enc Dalloz civ', 'Enc Dalloz comm', 'Enc Dalloz crim', 'Enc Dalloz intern', 'Enc Dalloz proc civ', 'Enc Dalloz publ', 'Enc Dalloz soc trav', 'Enc dir', 'Enc giur', 'Enc giur it', 'Enc it', 'NDI', 'N rép Dalloz', 'Nss DI', 'Rép Dalloz', 'Tr dir amm eur', 'Trattato Orlando', 'Acque bon costr', 'Adl', 'Adm Law Rev', 'Adm publ', 'Adm Soc', 'Aff esteri', 'Aff soc internaz', 'Agg soc', 'Agr it', 'Ambiente', 'Am journ compar Law', 'Amm civ', 'Amm giud', 'Ammin', 'Ann Bari', 'Ann Camerino', 'Ann Catania', 'Ann dir intern', 'Ann Ferrara', 'Ann Genova', 'Ann Macerata', 'Ann Messina', 'Ann Palermo', 'Ann Perugia', 'Ann pi', 'Ann Rev Pol Sc', 'Ann sem Palermo', 'Ann stor dir', 'Ann triestini', 'Ann dir comp', 'Anr inst dr intern', 'App urb edil',

            'Arch civ', 'Arch dir eccl', 'Arch dir pubbl', 'Arch fin', 'Arch giur', 'Arch giur circ', 'Arch giur op pubbl', 'Arch loc', 'Arch nuova proc pen', 'Arch pen', 'Arch ph dr', 'Arch resp civ', 'Arch ric giur', 'Arch st corp', 'Arch stor it', 'Arch ven', 'Asi', 'Assic', 'Assist soc', 'Azienda pubbl', 'Azienditalia', 'Banca borsa', 'Banca borsa tit cred', 'Banca cred', 'Banca impr soc', 'Boll ass cass risp', 'Boll ass it dir maritt', 'Boll cost parl', 'Boll for', 'Boll legisl tecn', 'Boll tribut', 'Br intern Law', 'Bull soc leg comp', 'Cahiers dr europ', 'Cal giud', 'Can Bar Rev', 'Carte stor', 'Cass pen', 'Cass pen mass', 'CIJ Recueil', 'Circ trasp', 'Civ catt', 'Comm Market Rep', 'Comm Market Rev', 'Comp stat it', 'Comuni dIt', 'Comun intern', 'Contr', 'Contr impr', 'Contr impr eur', 'Controllo legale conti', 'Corr trib', 'Corr giur', 'Crit dir', 'Crit econ', 'Crit pen', 'Crit pol', 'Crit soc', 'Crit sociol', 'Danno resp', 'D&G', 'Dem dir', 'Dif contr', 'Dir amm', 'Dir aut', 'Dir autom', 'Dir banca mercato fin', 'Dir b pubbl', 'Dir comm', 'Dir commercio intern', 'Dir comunit scambi intern', 'Dir eccl', 'Dir ed econ', 'Dir econ assic', 'Dir fall', 'Dir gest ambiente', 'Dir giur', 'Dir giur agr amb', 'Dir immigraz cittadinanza', 'Dir ind', 'Dir intern', 'Dir intern bell', 'Dir lav', 'Dir merc lav', 'Dir militare', 'Dir pen processo', 'Dir prat assic', 'Dir prat comm', 'Dir prat lav', 'Diritto prat soc', 'Dir prat tribut', 'Dir priv', 'Dir proc amm', 'Dir pubbl', 'Dir pubbl comp eur', 'Dir pubbl reg', 'Dir relaz ind', 'Dir san', 'Dir soc', 'Dir trasp', 'Dir Ue', 'Disciplina comm', 'DL Riv critica dir lav', 'Doc it', 'Dr adm', 'Dr inform tel', 'Droits', 'DVerw', 'Econ ambiente', 'Econ cultura', 'Econ intern', 'Econ dir terz', 'Econ intern en', 'Econ it', 'Econ pubbl', 'Ed cred', 'Ed terr', 'Enc com', 'Energia', 'Enti pubbl', 'Es imp dir fisc', 'EDCE', 'ER',

            'Europa dir priv', 'Eur Competition Law Rev', 'Eur Law J', 'Eur Law Rev', 'Eur Leg For', 'Fin loc', 'Fordham Intern Law J', 'Foro ambr', 'Foro amm Tar', 'Foro it', 'Foro nap', 'Foro Sal', 'Foro tosc', 'Funz pubbl', 'Gazz ambiente', 'Gazz giur', 'Gazz loc', 'Gazz tribut', 'Giorn dir amm', 'Giorn dir lav rel ind', 'Giorn stor cost', 'Giur agr it', 'Giur annotata dir ind', 'Giur boll legisl tecn', 'Giur comm', 'Giur comp dir civ', 'Giur compl cass civ', 'Giur compl cass pen', 'Giur Corti reg', 'Giur cost', 'Giur imp', 'Giur imp dr', 'Giur imp dr reg neg', 'Giur infort', 'Giur it', 'Giur lav', 'Giur merito', 'Giur milanese', 'Giur napoletana', 'Giur op pubbl', 'Giur piemontese', 'Giur romana', 'Giur samm', 'Giur sic', 'Giur tor', 'Giur tosc', 'Gius', 'Giust', 'Giust amm', 'Giust amm sic', 'Giust autom', 'Giust civ', 'Giust civ mass app', 'Giust civ Mass', 'Giust Cost', 'Giust fin', 'Giustit', 'Giust pen', 'Gov', 'GT riv giur trib', 'Guida dir', 'Guida el', 'Guida lav', 'Harv Intern Law J', 'Harv Law Rev', 'Hastings Intern Comp Law Rev', 'Hist J', 'Ig sic lav', 'Imp cons entr', 'Impos dr', 'Industria', 'Ind sind', 'Informatica dir', 'Informatica doc', 'Informaz previd', 'Inf tribut comm', 'Intern Law Pol', 'Intern Law Quart', 'Intern Law Rev', 'Intern Leg Mat', 'Intern J Soc Law', 'Intern Org', 'Ist federalismo', 'J dr intern', 'J dr intern priv', 'J eur Verw', 'J intern ausl öff R', 'J öff R', 'J Comm Market St', 'J Econ Liter', 'J Env Law', 'J Eur Publ Pol', 'J Inst Th Econ', 'J Leg St', 'J Pubbl Pol', 'Lav dir', 'Lav nella giur', 'Lav prev oggi', 'Lav rel ind', 'Lav sicur soc', 'Law Pol', 'Leg it',

            'Leg pen', 'Leg St', 'Mass Ced', 'Mass dec pen', 'Mass giur it', 'giur', 'it', 'Mass giur lav', 'Mass Pen', 'Mass tribut', 'Mass trib supr mil', 'Mat stor cultura giur', 'MCR', 'Meridiana', 'Min it', 'Mn cred', 'Mom leg', 'Mon eccl', 'Mon pret', 'Mon trib', 'Mondo banc', 'Mondo giud', 'N arch ven', 'N dir', 'N dir agr', 'N leg pen', 'Nomos', 'Nord sud', 'Not giur lav', 'Not giur reg', 'Not fall', 'Not lav prev', 'N rass amm com', 'N rass ldg', 'N giur civ commentata', 'N riv app', 'N riv dir comm', 'N riv tribut', 'Nuove leggi civili', 'Or econ', 'Orient giur lav', 'Oss leg', 'Parl Aff', 'Pitts LR', 'Polis', 'Pol', 'Pol dir', 'Pol man publ', 'Pol Q', 'Pol parl', 'Pol Sc', 'Pol St', 'Poste tel', 'Pouvoirs', 'PQM', 'Prat az', 'Prevenz infort', 'Prev soc', 'Prev soc agr', 'PSQ', 'Prob amm pubbl', 'Prob Inf', 'Publ Adm', 'Publ Adm Dev', 'Publ Adm Rev', 'Publ Choice', 'Publ Law', 'Publ Pol Adm', 'Quad amm', 'Quad cost', 'Quad dir lav rel ind', 'Quad fior stor', 'Quad min fin', 'Quad reg', 'Quad sc pol', 'Racc Cons St', 'Racc giur C giust', 'Ragiufarm', 'Ragiusan', 'Rass amm sanita', 'Rass amm scuola', 'Rass arch St', 'Rass avv Napoli', 'Rass Avv St', 'Rass circ str', 'Rass dir cinem', 'Rass dir civ', 'Rass dir dog', 'Rass dir eccl', 'Rass dir farm', 'Rass dir pubbl', 'Rass dir san', 'Rass econ', 'Rass fin pubbl', 'Rass fin tribut', 'Rass forense', 'Rass giur Enel', 'Rass giur en elettr', 'Rass giur ist st leg', 'Rass giur s', 'Rass giur u', 'Rass giust mil', 'Rass imp dr', 'Rass lav', 'Rass lav pubbl', 'Rass parl', 'Rass penit crim', 'Rass penit criminol', 'Rass propr ila', 'Rass stat lav', 'Rass st penit', 'Rass tribut', 'Reg', 'Rel intern', 'Resp civ prev', 'Resp comunic impr', 'Rev adm publica', 'Rev der com eur', 'Rev es der adm', 'Rev esp der intern', 'Rev adm', 'Rev adm publ', 'Rev affaires europe´ennes', 'Rev crit dr intern priv', 'Rev dr intern dr comp', 'Rev dr intern leg comp', 'Rev dr intern sc dipl pol', 'Rev dr publ', 'Rev dr Ue', 'Rev eur dr publ', 'Rev fr adm publ', 'Rev fr dr adm', 'Rev fr sc pol', 'Rev ge´n dr', 'Rev ge´n dr intern', 'Rev hist dr fr', 'Rev intern dr comp', 'Rev intern sc adm', 'Rev MCUE', 'Rev MUE', 'Rev pol parl', 'Rev trim dr civ', 'Rev trim dr comm', 'Rev trim dr europ', 'Rev trim dr homme',

            'RHD', 'Riv Ag terr', 'Riv amm', 'Riv amm app', 'Riv arbitrato', 'Riv banc', 'Riv cass risp', 'Riv cat', 'Riv C conti', 'Riv coop', 'Riv crit dir giur', 'Riv crit dir priv', 'Riv crit infort lav', 'Riv dem', 'Riv dif soc', 'Riv dip terr', 'Riv dir aer', 'Riv dir agr', 'Riv dir civ', 'Riv dir comm', 'Riv dir cost', 'Riv dir eur', 'Riv dir fin', 'Riv dir impresa', 'Riv dir ind', 'Riv dir intern', 'Riv dir intern comp lav', 'Riv dir intern priv proc', 'Riv dir ipot', 'Riv dir lav', 'Riv dir lav mass', 'Riv dir matr', 'Riv dir mil', 'Riv dir miner', 'Riv dir nav', 'Riv dir penit', 'Riv dir priv', 'Riv dir proc', 'Riv dir proc civ', 'Riv dir proc pen', 'Riv dir pubbl', 'Riv dir sic soc', 'Riv dir sport', 'Riv dir tribut', 'Riv dott comm', 'Riv econ Mezz', 'Riv es forz', 'Riv fil', 'Riv fisc', 'Riv giur ambiente', 'Riv giur circ trasp', 'Riv giur edil', 'Riv giur lav', 'Riv giur magistr', 'Riv giur Mezz', 'Riv giur polizia', 'Uff', 'Vol', 'PQM', 'nr', 'est', 'cf', 'segg', 'sigg ri', 'tg', 'PM', 'cpa', 'iva', 'loc', 'cp c', 'cod proc civ', 'gd', 'sp', 'Disp Att', 'GIP', 'GUP', 'CC', 'CPC', 'CP', 'CPP', 'pubbl', 'ar', 'ab', 'ab km²', 'ab kmq', 'abbigl', 'abbrev', 'abl', 'aC', 'ac', 'acc', 'accorc', 'accr', 'adatt', 'adr', 'aeron', 'aerodin', 'affl', 'agg', 'agr', 'agric', 'alch', 'alg', 'alim', 'allev', 'allus', 'alt',

            'anat', 'anat comp', 'ant', 'antifr', 'anton', 'antrop', 'ar', 'arald', 'arc', 'arch', 'archeol', 'aritm', 'arred', 'art', 'arti min', 'artig', 'artigl', 'artt', 'assic', 'assol', 'astr', 'astrol', 'astron', 'att', 'attrav', 'aus', 'autom', 'avv', 'avvers', 'bal', 'ball', 'batt', 'bibl', 'bioch', 'biol', 'bot', 'bur', 'ca', 'cal', 'cap', 'capol', 'card', 'caus', 'ca', 'cm', 'cd', 'cd', 'a', 'centr', 'cfr', 'chim', 'chim ind', 'chir', 'ciber', 'cin', 'citol', 'class', 'cm²', 'cmq', 'cod civ',

            'enol', 'entom', 'epigr', 'es', 'escl', 'estens', 'estr min', 'etim', 'etn', 'etol', 'eufem', 'fam', 'farm', 'ferr', 'fig', 'filat', 'filol', 'filos', 'fin', 'fis', 'fisiol', 'fisiopatol', 'folcl', 'fon', 'fotogr', 'fr', 'fut', 'gen', 'geneal', 'geod', 'geofis', 'geogr', 'geogr antr', 'geogr fis', 'geol', 'geom', 'germ', 'giorn', 'gr', 'gram', 'ibid', 'icon', 'id', 'idraul', 'ig', 'imp', 'imper', 'imperf', 'impers', 'ind', 'ind agr', 'ind alim', 'ind cart', 'ind chim', 'ind cuoio', 'ind estratt', 'ind graf', 'ind mecc', 'ind tess', 'indecl', 'indef', 'indeterm', 'inf', 'inform', 'ing', 'ingl', 'ins', 'inter', 'intr', 'invar', 'iron', 'irreg', 'is', 'istol', 'it', 'ittiol', 'lat', 'lav femm', 'lav pubbl', 'lett', 'ling', 'lit', 'loc', 'loc div', 'long', 'macch', 'mar', 'mat', 'mater', 'max', 'mecc', 'med', 'mediev', 'merc', 'merid', 'metall', 'meteor', 'metr', 'metrol', 'microb', 'mil', 'min', 'miner', 'mitol', 'mod', 'morf', 'mss', 'mus', 'na', 'neg', 'neol', 'neur', 'nom', 'numism', 'oc', 'occ', 'occult', 'oculist', 'od', 'ogg', 'oland', 'onomat', 'ord', 'ord scol', 'oref', 'orient', 'ornit', 'orogr', 'ott', 'pa', 'pag', 'pagg', 'paleobot', 'paleogr', 'paleont', 'paleozool', 'paletn', 'papir', 'parapsicol', 'part', 'partic', 'pass', 'patol', 'pedag', 'pegg', 'per ind', 'pers',

            'petr', 'petrogr', 'pitt', ' D.Lgs', 'pl', 'poet', 'pol', 'popol', 'port', 'poss', 'pr', 'pref', 'preist', 'prep', 'pres', 'pret', 'priv', 'prof', 'pron', 'pronom', 'propr', 'prov', 'prox', 'psicoan', 'psicol', 'qc', 'qlc', 'qlcn', 'qlco', 'qlcs', 'qlcu', 'qualif', 'radiotecn', 'rag', 'rar', 'recipr', 'reg', 'region', 'rel', 'rem', 'rep', 'retor', 'rifl', 'rit', 'rom', 'scherz', 'scien', 'scult', 'sec', 'secc', 'seg', 'segg', 'sigill', 'sig', 'sigg', 'sig ra', 'sig na', 'simb', 'sin', 'sing', 's/m', 'sociol', 'sogg', 'sp', 'spett', 'spreg', 'st', 'stat', 'st d arte', 'st d dir', 'st d filos', 'st d rel', 'suff', 'sup', 'superl', 'tav', 'tecn', 'tecnol', 'ted', 'tel', 'telecom', 'temp', 'teol', 'term', 'tess', 'tipogr', 'top', 'topog', 'tosc', 'tr', 'trad', 'trasp', 'ungh', 'urban', 'val', 'vd', 'veter', 'vezz', 'voc', 'vol', 'volg', 'voll', 'zool', 'zoot', 'disp att cc', 'succ mod', 'ss mm ii', 'co', 'D Lgs', 'cd', 'sez un', 'sent', 'nn', 't a r', 'rv', 'ric n', 'fall', 'l c a', 't u b', 'Cass civ'
        ])

        
        self.add_custom_rules(self.nlp)

    def tokenize(self, text: str, min_token: int) -> List[str]:
        doc = self.nlp(text)
        italian_words = []

        for token in doc:
            if (token.is_alpha and
                token.lang_ == 'it' and
                token.text.lower() not in self.abbreviations and
                    len(token.text) > min_token):  # Exclude single-letter words
                italian_words.append(token.text.lower())

        return italian_words

    def extract_entities(self, text):
        return self.entity_extractor.extract_entities(text)

    def add_custom_rules(self, nlp):
        for abbr in self.abbreviations:
            nlp.tokenizer.add_special_case(abbr + '.', [{'ORTH': abbr + '.'}])

        if 'sentencizer' not in nlp.pipe_names:
            nlp.add_pipe('sentencizer', before='parser')

        nlp.add_pipe('custom_sentence_segmentation', after='sentencizer')

    @staticmethod
    @Language.component("custom_sentence_segmentation")
    def custom_sentence_segmentation(doc):
        abbreviations = Chunker.get_instance().abbreviations
        for token in doc[:-1]:
            if token.text.lower().rstrip('.') in abbreviations:
                token.nbor(1).is_sent_start = False
            elif re.match(r'^\d+\.$', token.text) and token.i + 1 < len(doc):
                # Check if it's a numbered list item
                next_token = doc[token.i + 1]
                if next_token.is_alpha and next_token.is_title:
                    # Likely a list item, don't split
                    next_token.is_sent_start = False
            elif token.text == ':':
                # Check context for colon
                prev_token = doc[token.i - 1] if token.i > 0 else None
                next_token = doc[token.i + 1] if token.i + \
                    1 < len(doc) else None

                # Don't split if colon is used in time expressions
                if prev_token and next_token and prev_token.is_digit and next_token.is_digit:
                    next_token.is_sent_start = False
                # Don't split if colon is used after a single word (likely introducing a list or explanation)
                elif prev_token and len(prev_token.sent) <= 2:
                    next_token.is_sent_start = False
        return doc

    def read_file(self, doc_location):
        _, file_extension = os.path.splitext(doc_location)
        text = ""

        if file_extension.lower() == '.pdf':
            text = self.read_pdf(doc_location)
        else:
            text = self.read_text(doc_location)

        if not text.strip():  
            raise ValueError(f"The document {doc_location} is empty.")

        return text

    def read_pdf(self, doc_location):
        text = ""
        with fitz.open(doc_location) as doc:
            for page in doc:
                text += page.get_text()
        return text

    def read_text(self, doc_location):
        try:
            with open(doc_location, 'r', encoding='utf-8') as file:
                return file.read()
        except UnicodeDecodeError:
            with open(doc_location, 'r', encoding='iso-8859-1') as file:
                return file.read()

    def split_into_sentences(self, text):
        doc = self.nlp(text)
        sentences = [sent.text.strip() for sent in doc.sents]

        if len(sentences) == 0:  
            raise ValueError("No sentences found in the document.")

        return sentences

    def chunk_text(self, doc_location: str) -> List[Dict]:
        logger.info(f"Starting to chunk document: {doc_location}")

        text = self.read_file(doc_location)
        logger.info(f"Extracted text length: {len(text)}")
        logger.debug(f"First 200 characters of extracted text: {text[:200]}")

        sentences = self.split_into_sentences(text)
        logger.info(f"Number of sentences: {len(sentences)}")

        sentences = [s for s in sentences if s.strip()]

        if not sentences:
            logger.warning(
                f"No valid sentences found in document: {doc_location}")
            return []

        chunks = []
        i = 0
        total_sentences = len(sentences)
        while i < total_sentences:
            chunk_sentences = sentences[i:i + self.max_sentences_per_chunk]
            chunk_text = ' '.join(chunk_sentences)

            entities = self.extract_entities(chunk_text)

            chunk_embedding = self.embedder.get_embedding(chunk_text)

            chunks.append({
                'text': chunk_text,
                'embedding': chunk_embedding,
                'entities': entities
            })

            logger.debug(
                f"Created chunk with {len(chunk_sentences)} sentences")
            i += self.max_sentences_per_chunk

        logger.info(
            f"Created {len(chunks)} chunks for document: {doc_location}")
        return chunks
