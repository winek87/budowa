# plik: core/ai_tagger/model_handler.py (WERSJA Z WŁASNYM FOLDEREM CACHE)
# -*- coding: utf-8 -*-

import logging
import asyncio
import json
from pathlib import Path
from typing import List, Dict, Union

try:
    from googletrans import Translator
    GOOGLETRANS_AVAILABLE = True
except ImportError:
    GOOGLETRANS_AVAILABLE = False

from rich.console import Console
# === ZMIANA 1: Importujemy nową zmienną konfiguracyjną ===
from ..config import (
    AI_TAGGER_CONFIDENCE_THRESHOLD, 
    AI_TAGGER_TRANSLATION_MODE,
    AI_MODELS_CACHE_DIR # <--- NOWY IMPORT
)

console = Console()
logger = logging.getLogger("app")

class AITagger:
    """Zarządza ładowaniem modelu AI, przetwarzaniem obrazów i generowaniem tagów."""
    def __init__(self, model_path: str):
        self.model_path = model_path
        # ... (reszta __init__ bez zmian) ...
        self.processor = None
        self.model = None
        self.device = None
        self.Image = None
        self.UnidentifiedImageError = None
        self.AutoImageProcessor = None
        self.AutoModelForImageClassification = None
        self.torch = None
        logger.info(f"Inicjalizacja instancji AI Tagger dla modelu: {self.model_path}")


    # ... (_dynamic_import_ai_libs bez zmian) ...
    async def _dynamic_import_ai_libs(self):
        if self.torch: return
        try:
            from PIL import Image, UnidentifiedImageError
            from transformers import AutoImageProcessor, AutoModelForImageClassification
            import torch
            self.Image, self.UnidentifiedImageError = Image, UnidentifiedImageError
            self.AutoImageProcessor, self.AutoModelForImageClassification = AutoImageProcessor, AutoModelForImageClassification
            self.torch = torch
        except ImportError as e:
            logger.critical(f"Brak kluczowych bibliotek AI: {e}")
            raise RuntimeError("Nie można załadować bibliotek AI (torch, transformers, Pillow).") from e


    async def load_model(self):
        """Ładuje model i procesor AI do pamięci, używając niestandardowego folderu cache."""
        if self.model and self.processor: return
        await self._dynamic_import_ai_libs()
        
        with console.status(f"[cyan]Ładowanie modelu AI '{self.model_path}'...[/]"):
            try:
                def _load_from_hub():
                    if self.torch.cuda.is_available(): device = self.torch.device("cuda")
                    elif hasattr(self.torch.backends, 'mps') and self.torch.backends.mps.is_available(): device = self.torch.device("mps")
                    else: device = self.torch.device("cpu")

                    # Upewnij się, że katalog cache istnieje
                    cache_path = Path(AI_MODELS_CACHE_DIR)
                    cache_path.mkdir(parents=True, exist_ok=True)
                    
                    # === ZMIANA 2: Używamy argumentu 'cache_dir' ===
                    processor = self.AutoImageProcessor.from_pretrained(
                        self.model_path, 
                        cache_dir=str(cache_path)
                    )
                    model = self.AutoModelForImageClassification.from_pretrained(
                        self.model_path, 
                        cache_dir=str(cache_path)
                    ).to(device)
                    # === KONIEC ZMIANY ===

                    return processor, model, device
                
                self.processor, self.model, self.device = await asyncio.to_thread(_load_from_hub)
                logger.info(f"Model {self.model_path} załadowany. Używane urządzenie: {self.device}")
            except Exception as e:
                raise RuntimeError(f"Błąd podczas ładowania modelu {self.model_path}: {e}")

    # ... (tag_images_batch bez zmian) ...
    async def tag_images_batch(self, image_paths: List[Path]) -> Dict[Path, List[Dict]]:
        if not self.model or not self.processor: raise RuntimeError("Model AI nie jest załadowany.")
        def _process_batch():
            valid_images, valid_paths = [], []
            for path in image_paths:
                try:
                    img = self.Image.open(path).convert("RGB")
                    valid_images.append(img); valid_paths.append(path)
                except (self.UnidentifiedImageError, ValueError) as e:
                    logger.warning(f"Pominięto niepoprawny obraz: {path.name} ({e})")
            if not valid_images: return {}
            inputs = self.processor(images=valid_images, return_tensors="pt").to(self.device)
            with self.torch.no_grad(): outputs = self.model(**inputs)
            probs = self.torch.nn.functional.softmax(outputs.logits, dim=-1); top_k = self.torch.topk(probs, 5)
            results, all_en_tags, batch_meta = {}, set(), []
            for i, path in enumerate(valid_paths):
                tags = []
                for score, idx in zip(top_k.values[i], top_k.indices[i]):
                    if score.item() > AI_TAGGER_CONFIDENCE_THRESHOLD:
                        label = self.model.config.id2label[idx.item()].replace("_", " ")
                        tags.append({'label': label, 'score': score.item()}); all_en_tags.add(label)
                batch_meta.append({'path': path, 'tags': tags})
            trans_map = {}
            if AI_TAGGER_TRANSLATION_MODE == 'polish' and GOOGLETRANS_AVAILABLE and all_en_tags:
                try:
                    translator = Translator(); joined = " ||| ".join(all_en_tags)
                    translated = translator.translate(joined, src='en', dest='pl').text
                    trans_tags = [t.strip().lower() for t in translated.split('|||')]
                    if len(all_en_tags) == len(trans_tags): trans_map = dict(zip(all_en_tags, trans_tags))
                except Exception as e: logger.warning(f"Błąd tłumaczenia: {e}. Używam angielskich tagów.")
            for item in batch_meta:
                final_tags = [{'label': trans_map.get(t['label'], t['label']), 'score': t['score']} for t in item['tags']]
                if final_tags:
                    final_tags.sort(key=lambda x: x['score'], reverse=True)
                    results[item['path']] = final_tags
            return results
        return await asyncio.to_thread(_process_batch)
