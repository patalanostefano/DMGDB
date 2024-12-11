import os
import re
from pathlib import Path
from typing import List, Dict, Tuple

class NormativeTextCleaner:
    def __init__(self, text: str):
        self.text = text
        self.cleaned_text = ""
        self.sections = []

    def clean(self) -> str:
        """Pipeline principale di pulizia"""
        self.text = self._remove_updates()
        self.text = self._remove_double_parentheses()
        self.text = self._normalize_text()
        self.text = self._structure_text()
        return self.text

    def _remove_updates(self) -> str:
        """Rimuove selettivamente gli aggiornamenti preservando informazioni importanti"""
        patterns = [
            r'AGGIORNAMENTO.*?(?=\n\n)',
            r'-{3,}.*?-{3,}',
            r'Vigente al.*?\n',
            r'\({2}.*?\){2}',
        ]
        
        text = self.text
        text = re.sub(r'\([0-9]+\)', lambda m: m.group(0) if re.search(r'Art\.?\s*\d+.*' + re.escape(m.group(0)), text) else '', text)
        
        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.DOTALL | re.MULTILINE)
        return text

    def _remove_double_parentheses(self) -> str:
        """Rimuove contenuto tra doppie parentesi preservando numeri di articoli"""
        text = self.text
        parts = []
        last_end = 0
        for match in re.finditer(r'\(\((.*?)\)\)', text):
            start, end = match.span()
            content = match.group(1)
            if not re.match(r'Art\.?\s*\d', content):
                parts.append(text[last_end:start])
            else:
                parts.append(text[last_end:end])
            last_end = end
        parts.append(text[last_end:])
        return ''.join(parts)

    def _normalize_text(self) -> str:
        """Normalizza il testo mantenendo la struttura"""
        text = self.text

        header_match = re.match(r'DECRETO LEGISLATIVO.*?(?=\n\n)', text, re.DOTALL)
        if header_match:
            header = header_match.group(0)
            header = re.sub(r'\s+', ' ', header).strip()
            text = text[len(header_match.group(0)):]
            text = header + "\n\n" + text

        structural_patterns = [
            r'(CAPO [IVXLC]+.*?)\n',
            r'(SEZIONE [IVXLC]+.*?)\n',
            r'(PARTE [IVXLC]+.*?)\n',
            r'(TITOLO [IVXLC]+.*?)\n',
        ]
        for pattern in structural_patterns:
            text = re.sub(pattern, r'\1\n', text)

        text = re.sub(r' +', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        text = '\n'.join(line.rstrip() for line in text.splitlines())

        return text

    def _format_article(self, content: list[str]) -> list[str]:
        """Formatta un articolo con la rubrica su una nuova riga"""
        if not content:
            return []

        article_pattern = r'^Art\.*\s*(\d+(?:[-\s]?(?:bis|ter|quater|quinquies|sexies|septies|octies|novies|decies))?(?:\s*\.\s*\d+)?)\s*(.*)$'
        article_match = re.match(article_pattern, content[0], re.IGNORECASE)
        
        if not article_match:
            return content

        article_num = article_match.group(1)
        article_title = article_match.group(2)

        article_num = re.sub(r'\s*(bis|ter|quater|quinquies|sexies|septies|octies|novies|decies)', r' \1', article_num, flags=re.IGNORECASE)
        article_num = re.sub(r'\s*\.\s*', '.', article_num)
        
        formatted = [f"Art. {article_num.strip()}"]
        if article_title:
            formatted.append(article_title.strip())

        # Formatta i commi
        in_comma = False
        for line in content[1:]:
            line = line.strip()
            if re.match(r'^\d+\.', line):  # Nuovo comma
                formatted.append('')  
                formatted.append(line)
                in_comma = True
            elif line and in_comma:  # Contenuto del comma
                formatted.append(line)

        return formatted

    def _structure_text(self) -> str:
        """Struttura il testo secondo la gerarchia normativa"""
        lines = self.text.split('\n')
        structured_text = []
        
        patterns = {
            'PARTE': r'^PARTE\s+[IVXLC]+',
            'LIBRO': r'^LIBRO\s+[IVXLC]+',
            'TITOLO': r'^TITOLO\s+[IVXLC]+',
            'CAPO': r'^CAPO\s+[IVXLC]+',
            'SEZIONE': r'^SEZIONE\s+[IVXLC]+',
            'ARTICOLO': r'^Art\.*\s*\d+'
        }

        current_section = None
        buffer = []

        for line in lines:
            if not line.strip():
                if buffer:
                    structured_text.extend(self._format_section(current_section, buffer))
                    buffer = []
                continue

            section_type = None
            for type_name, pattern in patterns.items():
                if re.match(pattern, line, re.IGNORECASE):
                    section_type = type_name
                    break

            if section_type:
                if buffer:
                    structured_text.extend(self._format_section(current_section, buffer))
                    buffer = []
                current_section = section_type
            
            buffer.append(line)

        if buffer:
            structured_text.extend(self._format_section(current_section, buffer))

        return '\n'.join(line for line in structured_text if line is not None)

    def _format_section(self, section_type: str, content: list[str]) -> list[str]:
        """Formatta una sezione in base al suo tipo"""
        if not section_type:
            return content

        if section_type == 'ARTICOLO':
            return self._format_article(content)
        else:
            return self._format_hierarchical_section(section_type, content)

    def _format_hierarchical_section(self, section_type: str, content: list[str]) -> list[str]:
        """Formatta una sezione gerarchica"""
        if not content:
            return []

        header = content[0].strip().upper()
        formatted = [f"{header}"]
        
        if len(content) > 1:
            formatted.append('')
            formatted.extend(line.rstrip() for line in content[1:] if line.strip())
        
        return formatted

def process_files():
    """Processa tutti i file dalla cartella testi_normativi e salva i risultati in testi_puliti"""
    output_dir = Path("testi_puliti")
    output_dir.mkdir(exist_ok=True)
    
    input_dir = Path("testi_normativi")
    if not input_dir.exists():
        print("La cartella 'testi_normativi' non esiste. Creala e inserisci i file da processare.")
        return

    files_processed = 0
    for input_file in input_dir.glob("*.txt"):
        try:
            with open(input_file, "r", encoding="utf-8") as f:
                text = f.read()
            
            cleaner = NormativeTextCleaner(text)
            cleaned_text = cleaner.clean()
            
            output_filename = input_file.stem + "_pulito.txt"
            output_file = output_dir / output_filename
            

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(cleaned_text)
                
            print(f"Processato con successo: {input_file.name} -> {output_file.name}")
            files_processed += 1
            
        except Exception as e:
            print(f"Errore nel processare {input_file.name}: {str(e)}")
    
    if files_processed == 0:
        print("Nessun file .txt trovato nella cartella 'testi_normativi'.")
    else:
        print(f"\nProcesso completato. {files_processed} file elaborati.")

if __name__ == "__main__":
    process_files()