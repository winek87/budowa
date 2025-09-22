# plik: core/visual_duplicate_finder/tasks.py
# Wersja 1.0 - Czysta logika biznesowa dla Wyszukiwarki Duplikatów.
# Opis: Ten moduł zawiera funkcje do obliczania hashy percepcyjnych,
#       wyszukiwania podobnych par i zarządzania nimi w bazie danych.
# -*- coding: utf-8 -*-

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

# Leniwe ładowanie
Image, UnidentifiedImageError, imagehash = None, None, None

# Importy z modułów projektu `core`
from ..database import (update_perceptual_hash_batch, get_all_perceptual_hashes)

# Inicjalizacja
logger = logging.getLogger(__name__)

def _initialize_heavy_deps():
    """Dynamicznie ładuje ciężkie biblioteki."""
    global Image, UnidentifiedImageError, imagehash
    if Image is None:
        from PIL import Image, UnidentifiedImageError
    if imagehash is None:
        import imagehash

async def calculate_and_save_hashes(
    images_to_process: List[Dict],
    display: "HashCalculationLiveDisplay"
):
    """
    Skanuje obrazy, oblicza hashe percepcyjne i zapisuje je w bazie.
    """
    _initialize_heavy_deps()
    updates_batch = []
    BATCH_SIZE = 100

    for record in images_to_process:
        img_path = Path(record['final_path'])
        if not await asyncio.to_thread(img_path.exists):
            display.update_progress("failed", img_path.name)
            continue
        try:
            def calculate_hash():
                with Image.open(img_path) as img:
                    return imagehash.phash(img)
            p_hash = await asyncio.to_thread(calculate_hash)
            updates_batch.append((str(p_hash), record['id']))
            display.update_progress("success", img_path.name)
        except (UnidentifiedImageError, Exception) as e:
            logger.warning(f"Nie udało się obliczyć hasha dla {img_path.name}: {e}")
            display.update_progress("failed", img_path.name)

        if len(updates_batch) >= BATCH_SIZE:
            await update_perceptual_hash_batch(updates_batch)
            updates_batch = []
    
    if updates_batch:
        await update_perceptual_hash_batch(updates_batch)


async def find_similar_images(threshold: int, quick_scan: bool) -> List[Dict]:
    """
    Znajduje wizualnie podobne obrazy i zwraca grupy wraz z dystansem.
    
    Returns:
        List[Dict]: Lista słowników, gdzie każdy zawiera 'group' i 'distance'.
    """
    all_hashes_list = await get_all_perceptual_hashes()
    
    similar_groups, processed_ids = [], set()
    all_hashes_list.sort(key=lambda x: x.get('datetime') or datetime.min)

    for i, img1 in enumerate(all_hashes_list):
        if img1['id'] in processed_ids: continue

        current_group = [img1]
        distances = {} # Będziemy przechowywać dystans dla każdej pary

        for j in range(i + 1, len(all_hashes_list)):
            img2 = all_hashes_list[j]
            if img2['id'] in processed_ids: continue

            if quick_scan and img1['datetime'] and img2['datetime']:
                if (img2['datetime'] - img1['datetime']) > timedelta(minutes=5): break
            
            hash_str1, hash_str2 = img1.get("hash"), img2.get("hash")
            if not (isinstance(hash_str1, str) and isinstance(hash_str2, str)): continue
            
            hash1 = imagehash.hex_to_hash(hash_str1)
            hash2 = imagehash.hex_to_hash(hash_str2)
            
            distance = hash1 - hash2
            if distance <= threshold:
                current_group.append(img2)
                processed_ids.add(img2['id'])
                # Zapisujemy dystans między pierwszym elementem a każdym kolejnym
                distances[img2['id']] = distance

        if len(current_group) > 1:
            # Zwracamy grupę i dystans dla pierwszej znalezionej pary
            first_match_id = current_group[1]['id']
            similar_groups.append({
                "group": current_group,
                "distance": distances.get(first_match_id, 0)
            })

        processed_ids.add(img1['id'])
        
    return similar_groups
