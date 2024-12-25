import json
import re
import pandas as pd
from pathlib import Path
from datetime import datetime


# Пути для файлов
folder_path = Path('./file_buffer')  # В данной директории находятся файлы для проверки (JSON и XLSX)
output_folder = Path('./output_buffer')  # В эту директорию будет сохранен итоговый документ после завершения обработки.
output_folder.mkdir(exist_ok=True)


def convert_english_to_russian(plate):
    """
    Заменяет английские буквы на соответствующие русские символы в номере ГРЗ.
    """
    eng_to_rus = {
        'A': 'А', 'B': 'В', 'E': 'Е', 'K': 'К', 'M': 'М',
        'H': 'Н', 'O': 'О', 'P': 'Р', 'C': 'С', 'T': 'Т',
        'Y': 'У', 'X': 'Х'
    }
    return ''.join(eng_to_rus.get(char, char) for char in plate)


def load_and_clean_json_data(json_path):
    """Загружает JSON и удаляет региональные обозначения из номеров."""
    with open(json_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    cleaned_json_data = []
    for item in json_data:
        plate = item['plate'].replace(' RUS', '').replace(' KAZ', '').replace(' ABH', '')  # при желании можно будет собирать и работать с этими данными, можно считать количество иностранных ГРЗ
        cleaned_json_data.append({
            'plate': plate,
            'direction': item['direction'],
            'lp_template': item['lp_template'],
            'vehicle_type': item['vehicle_type'],
            'speed': item.get('speed', '0')
        })
    return cleaned_json_data


def load_and_clean_xlsx_data(xlsx_path, ignore_comments=True):
    """
    Загружает и очищает данные из XLSX.
    Если ignore_comments=False, не фильтрует записи в комментах XLSX`а с указанными словами 'не видно', 'отсутствует', 'трактор' в комментариях. Ignore_comments=False сильно вредит итоговой статистике.
    """
    df = pd.read_excel(xlsx_path, usecols=[0, 2, 3, 4, 5], header=0)
    df.columns = ['plate', 'direction', 'lp_template', 'vehicle_type', 'comment']

    df = df[df['plate'].notnull()]  # Исключаем строки без ГРЗ

    if ignore_comments:
        ignore_keywords = ['не видно', 'отсутствует', 'трактор']
        comment_filter = df['comment'].str.lower().apply(
            lambda x: not any(keyword in x for keyword in ignore_keywords) if isinstance(x, str) else True
        )
        df = df[comment_filter]  # Применяем фильтр комментов

    # Убираем колонку комментария, так как она больше не нужна
    df = df.drop(columns=['comment'])
    return df.to_dict('records')


def calculate_statistics(data, is_json=False):
    """Вычисляет статистику по типам ТС, направлениям, шаблонам ГРЗ и наличию символа сомнения (?)."""
    stats = {
        "total_detections": len(data),
        "questionable_plates": sum(1 for item in data if "?" in item['plate']),
        "vehicle_types": {
            "легковые": sum(1 for item in data if isinstance(item['vehicle_type'], str) and item['vehicle_type'].lower() == "легковой автомобиль"),
            "автобусы": sum(1 for item in data if isinstance(item['vehicle_type'], str) and item['vehicle_type'].lower() == "автобус"),
            "грузовые": sum(1 for item in data if isinstance(item['vehicle_type'], str) and item['vehicle_type'].lower() == "грузовой автомобиль"),
            "мотоциклы": sum(1 for item in data if isinstance(item['vehicle_type'], str) and item['vehicle_type'].lower() == "мотоцикл")
        },
        "directions": {
            "попутное": sum(1 for item in data if isinstance(item['direction'], str) and item['direction'].lower() == "попутное"),
            "встречное": sum(1 for item in data if isinstance(item['direction'], str) and item['direction'].lower() == "встречное"),
            "неизвестное": sum(1 for item in data if isinstance(item['direction'], str) and item['direction'].lower() == "неизвестное")
        },
        "lp_templates": {
            "такси": sum(1 for item in data if isinstance(item['lp_template'], str) and item['lp_template'].lower() == "такси"),
            "прицеп": sum(1 for item in data if isinstance(item['lp_template'], str) and item['lp_template'].lower() == "прицеп"),
            "военный": sum(1 for item in data if isinstance(item['lp_template'], str) and item['lp_template'].lower() == "военный"),
            "полиция": sum(1 for item in data if isinstance(item['lp_template'], str) and item['lp_template'].lower() == "полиция")
        }
    }

    # Расчет скорости только для JSON ибо в XLSX нет скорости.
    if is_json:
        stats["speed_detections"] = sum(1 for item in data if isinstance(item.get('speed', '0'), str) and item['speed'] != '0')
    else:
        stats["speed_detections"] = 0

    return stats


def is_similar(plate1, plate2, threshold=0.8):  # для дублей
    from difflib import SequenceMatcher
    """Проверяет, похожи ли два номера с учётом допустимого отклонения."""
    similarity = SequenceMatcher(None, plate1, plate2).ratio()
    return similarity >= threshold


def compare_data_with_errors_verbose(xlsx_data, json_data):
    """
    Оптимизированное сравнение данных XLSX и JSON, включает: поиск ошибок в типах ТС, шаблонах ГРЗ и ненайденных номеров.
    """
    vehicle_type_errors = []
    lp_template_errors = []
    plate_errors = []

    # Создается индекс для ускоренного поиска в JSON по "plate "и "direction"
    json_index = {
        (entry['plate'], entry['direction']): entry
        for entry in json_data
    }

    for xlsx_entry in xlsx_data:
        key = (xlsx_entry['plate'], xlsx_entry['direction'])
        matched_json_entry = json_index.get(key)

        if matched_json_entry:
            if xlsx_entry['vehicle_type'] != matched_json_entry['vehicle_type']:
                vehicle_type_errors.append({
                    "xlsx": xlsx_entry,
                    "json": matched_json_entry,
                    "reason": f"Разный тип ТС: XLSX: {xlsx_entry['vehicle_type']} | JSON: {matched_json_entry['vehicle_type']}"
                })

            if xlsx_entry['lp_template'] != matched_json_entry['lp_template']:
                lp_template_errors.append({
                    "xlsx": xlsx_entry,
                    "json": matched_json_entry,
                    "reason": f"Разный шаблон ГРЗ: XLSX: {xlsx_entry['lp_template']} | JSON: {matched_json_entry['lp_template']}"
                })
        else:
            plate_errors.append({
                "xlsx": xlsx_entry,
                "reason": "ГРЗ не найдено в JSON"
            })

    return vehicle_type_errors, lp_template_errors, plate_errors


def is_near_duplicate(plate1, plate2):  # для дублей
    """
    Проверяет, являются ли номера почти дублями (похожесть номеров): Отличие максимум в 1-2 символах.
    """
    diff = sum(1 for a, b in zip(plate1, plate2) if a != b)
    diff += abs(len(plate1) - len(plate2))  # Смотрим и учитываем разницу в длине
    return diff <= 2


def find_duplicates(data, key="plate", direction_key="direction", distance=5, threshold=0.8): # для дублей
    """Ищет дубли в данных с учётом расстояния, "схожести" и направления."""
    duplicates = []
    plates = [record.get(key, "") for record in data]
    directions = [record.get(direction_key, "Неизвестное") for record in data]

    for i, plate in enumerate(plates):
        for j in range(i + 1, min(i + 1 + distance, len(plates))):
            if is_similar(plate, plates[j], threshold) or is_near_duplicate(plate, plates[j]):
                direction_i = directions[i]
                direction_j = directions[j]
                if direction_i == "Неизвестное" or direction_j == "Неизвестное" or direction_i == direction_j:
                    duplicates.append((plate, plates[j], direction_i, direction_j))
    return duplicates


def find_lp_template_errors_optimized(json_data, xlsx_data):
    """
    Эта функция для проверки ошибок в шаблонах ГРЗ между JSON и XLSX.
    Возвращает количество ошибок и список несовпадений.
    """
    xlsx_templates_dict = {
        convert_english_to_russian(row['plate']): row['lp_template'].lower()
        for row in xlsx_data
        if isinstance(row.get('lp_template'), str) and isinstance(row.get('plate'), str)
    }

    errors = 0
    mismatched_plates = []

    for record in json_data:
        plate = convert_english_to_russian(record.get('plate', ''))
        lp_template = record.get('lp_template', '').lower()

        expected_template = xlsx_templates_dict.get(plate)
        if expected_template and lp_template != expected_template:
            errors += 1
            mismatched_plates.append({
                "plate": plate,
                "expected_template": expected_template,
                "actual_template": lp_template
            })

    return errors, mismatched_plates


def compare_files(json_data, xlsx_data):
    """Сравнивает данные JSON и XLSX файлов."""
    json_plates = [record["plate"] for record in json_data]
    xlsx_plates = [record["plate"] for record in xlsx_data]

    in_json_not_in_xlsx = [plate for plate in json_plates if plate not in xlsx_plates]
    in_xlsx_not_in_json = [plate for plate in xlsx_plates if plate not in json_plates]

    return in_json_not_in_xlsx, in_xlsx_not_in_json


def generate_report(xlsx_stats, json_stats, vehicle_type_errors, lp_template_errors, plate_errors, xlsx_duplicates, json_duplicates, output_path, missing_json, missing_xlsx):
    """Создает отчет с собранной статистикой и разницей между XLSX и JSON."""
    report_data = {
        "Параметр": [
            "Общее количество детекций",
            "ГРЗ с символом сомнения",
            "Легковые",
            "Автобусы",
            "Грузовые",
            "Мотоциклы",
            "Попутное",
            "Встречное",
            "Такси",
            "Прицеп",
            "Военный",
            "Полиция",
            "Количество ненайденных ГРЗ",
            "Количество ошибок в определении типа ТС",
            "Количество ошибок в определении шаблона ГРЗ",
            "Детекции со скоростью",
            "Дубли"
        ],
        "Визуальный подсчет": [
            xlsx_stats["total_detections"],
            xlsx_stats["questionable_plates"],
            xlsx_stats["vehicle_types"]["легковые"],
            xlsx_stats["vehicle_types"]["автобусы"],
            xlsx_stats["vehicle_types"]["грузовые"],
            xlsx_stats["vehicle_types"]["мотоциклы"],
            xlsx_stats["directions"]["попутное"],
            xlsx_stats["directions"]["встречное"],
            xlsx_stats["directions"]["неизвестное"],
            xlsx_stats["lp_templates"]["такси"],
            xlsx_stats["lp_templates"]["прицеп"],
            xlsx_stats["lp_templates"]["военный"],
            xlsx_stats["lp_templates"]["полиция"],
            len(missing_xlsx),  # Ненайденные ГРЗ в XLSX
            vehicle_type_errors,  # Ошибки типа ТС в XLSX
            lp_template_errors,  # Ошибки шаблона ГРЗ в XLSX
            0,  # Скорость в XLSX отсутствует
            len(xlsx_duplicates)
        ],
        "Автоматизированный": [
            json_stats["total_detections"],
            json_stats["questionable_plates"],
            json_stats["vehicle_types"]["легковые"],
            json_stats["vehicle_types"]["автобусы"],
            json_stats["vehicle_types"]["грузовые"],
            json_stats["vehicle_types"]["мотоциклы"],
            json_stats["directions"]["попутное"],
            json_stats["directions"]["встречное"],
            json_stats["directions"]["неизвестное"],
            json_stats["lp_templates"]["такси"],
            json_stats["lp_templates"]["прицеп"],
            json_stats["lp_templates"]["военный"],
            json_stats["lp_templates"]["полиция"],
            len(missing_json),  # Ненайденные ГРЗ в JSON
            "",  # Ошибки типа ТС в JSON
            "",  # Ошибки шаблона ГРЗ в JSON
            json_stats["speed_detections"],  # Скорость из JSON
            len(json_duplicates)
        ],
        "% расхождения": [
            abs(xlsx_stats["total_detections"] - json_stats["total_detections"]) / xlsx_stats["total_detections"] * 100 if xlsx_stats["total_detections"] else 0,
            abs(xlsx_stats["questionable_plates"] - json_stats["questionable_plates"]) / xlsx_stats["questionable_plates"] * 100 if xlsx_stats["questionable_plates"] else 0,
            *[abs(xlsx_stats["vehicle_types"][key] - json_stats["vehicle_types"][key]) / xlsx_stats["vehicle_types"][key] * 100 if xlsx_stats["vehicle_types"][key] else 0 for key in xlsx_stats["vehicle_types"]],
            *[abs(xlsx_stats["directions"][key] - json_stats["directions"][key]) / xlsx_stats["directions"][key] * 100 if xlsx_stats["directions"][key] else 0 for key in xlsx_stats["directions"]],
            *[abs(xlsx_stats["lp_templates"][key] - json_stats["lp_templates"][key]) / xlsx_stats["lp_templates"][key] * 100 if xlsx_stats["lp_templates"][key] else 0 for key in xlsx_stats["lp_templates"]],
            len(missing_json) + len(missing_xlsx),  # Сумма ненайденных ГРЗ
            "",  # Общая ошибка типа ТС
            "",  # Общая ошибка шаблона ГРЗ
            "", ""
        ]
    }

    report_df = pd.DataFrame.from_dict(report_data, orient='index').transpose()
    report_df.to_excel(output_path, index=False)
    print(f"Отчет создан по пути: {output_path}")


if __name__ == '__main__':
    # Настройка параметров
    threshold = 0.8  # Порог схожести для сравнения номеров 0.8 - стандарт
    distance = 5  # Дистанция от строки для поиска дублей 4-5 - стандарт

    # Поиск файлов в указанной ранее папке
    json_path = next(folder_path.glob("*.json"))
    xlsx_path = next(folder_path.glob("*.xlsx"))

    # Загрузка данных и очищение от ненужных данных
    json_data = load_and_clean_json_data(json_path)
    xlsx_data = load_and_clean_xlsx_data(xlsx_path, ignore_comments=True)  # ignore_comments стандарт - True

    json_notin_xlsx, xlsx_notin_json = compare_files(json_data, xlsx_data)

    # Статистика
    xlsx_stats = calculate_statistics(xlsx_data)
    json_stats = calculate_statistics(json_data, is_json=True)

    # Ошибки сравнения
    vehicle_type_errors, lp_template_errors, plate_errors = compare_data_with_errors_verbose(      # lp_template_errors не используется. Вместо него lp_template_error_count
        xlsx_data, json_data
    )

    # Поиск дублей
    xlsx_duplicates = find_duplicates(xlsx_data, key='plate', direction_key='direction', distance=distance, threshold=threshold)
    json_duplicates = find_duplicates(json_data, key='plate', direction_key='direction', distance=distance, threshold=threshold)

    # Генерация отчета
    timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    output_path = output_folder / f'comparison_report_{timestamp}.xlsx'

    # Функция для проверки ошибок в шаблонах ГРЗ более корректно работает.
    lp_template_error_count, lp_template_mismatches = find_lp_template_errors_optimized(json_data, xlsx_data)

    # Добавляем результаты в отчет
    generate_report(
        xlsx_stats, json_stats, len(vehicle_type_errors), lp_template_error_count,
        len(plate_errors), xlsx_duplicates, json_duplicates, output_path,
        json_notin_xlsx, xlsx_notin_json
    )

