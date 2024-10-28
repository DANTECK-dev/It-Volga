import pandas as pd
import re
from rapidfuzz import fuzz, process
#from fuzzywuzzy import process
import csv
from collections import defaultdict

# Загрузка данных
addresses_file = "volgait2024-semifinal-addresses.csv"
tasks_file = "volgait2024-semifinal-task.csv"
results_file = "volgait2024-semifinal-result.csv"

# addresses_file = "volgait2024-semifinal-addresses-test.csv"
# tasks_file = "volgait2024-semifinal-task-test.csv"
# results_file = "volgait2024-semifinal-result-test.csv"

addresses_df = pd.read_csv(addresses_file, delimiter=';', encoding='utf-8')
tasks_df = pd.read_csv(tasks_file, delimiter=';', encoding='utf-8')

# Создание словаря для быстрого поиска UUID по адресу
address_to_uuid = dict(zip(addresses_df['house_full_address'], addresses_df['house_uuid']))

print(addresses_df.head())
print(tasks_df.head())


# Определим структуру для хранения данных
class IncidentsAddressesData:
    def __init__(self, shutdown_id: int, comment: str):
        self.remaining_words = None
        self.shutdown_id = shutdown_id
        self.comment = comment
        self.incidents = self.extract_incidents(comment)
        self.comment_without_incidents = self.remove_incidents(comment, self.incidents)
        self.remove_abbreviations()
        self.extract_addresses = self.extract_addresses()
        self.address_to_uuid = address_to_uuid
        self.found_addresses, self.found_uuids = self.find_closest_addresses()
        self.print()

    def extract_incidents(self, comment: str) -> list[str]:
        """Извлекает аварии из комментария"""
        # Регулярное выражение для нахождения аварий
        incident_pattern = r'\b(авария|отключение|проблемы|неисправность|регулятора|водопроводной|задвидки|монтаж' \
                           r'|давления|д=[0-9]+|д=[0-9]+мм|д-[0-9]+|д-[0-9]+мм|участка|зап.|трубы|в/с|работает' \
                           r'|водоканал|утечка|одпу|без|раб.||||||' \
                           r'|хвс|п/з|утечка|из-под|из|заглушки|колодца|кол-ца|замена|врезка|сети|=[0-9]+|-[0-9]+' \
                           r'|земли|впу|ремонт|задвижки|пониж\.\s*|давл\.\s*|задвиж\.\s*|техн\.\s*|откл\.\s*|с/о)\b'
        incidents = re.findall(incident_pattern, comment, re.IGNORECASE)
        replace_space = ["  " if incidents == " " else incidents for incidents in incidents]
        return list(dict.fromkeys(replace_space))

    def remove_incidents(self, comment: str, incidents: list[str]) -> str:
        """Удаляет аварийные записи из комментария"""
        comment_without_incidents = comment

        # Удаляем каждую аварийную запись, заменяя её на пустую строку
        for incident in incidents:
            # Заменяем саму фразу
            comment_without_incidents = re.sub(rf'\b{re.escape(incident)}\b', '', comment_without_incidents, flags=re.IGNORECASE)

            # Удаляем пробелы перед запятой и точкой, оставляя их
            comment_without_incidents = re.sub(r'\s+([.,])', r'\1', comment_without_incidents)  # Удаляем пробелы перед запятыми и точками

            # Удаляем двойные точки, запятые, точки с запятой и запятые с точками
            comment_without_incidents = re.sub(r'\.{2,}', '.', comment_without_incidents)  # Двойные точки
            comment_without_incidents = re.sub(r',{2,}', ',', comment_without_incidents)  # Двойные запятые
            comment_without_incidents = re.sub(r';{2,}', ';', comment_without_incidents)  # Двойные точки с запятой
            comment_without_incidents = re.sub(r'(\.)(,)', r'\1', comment_without_incidents)  # Точка и запятая
            comment_without_incidents = re.sub(r'(,)(\.)', r'\1', comment_without_incidents)  # Запятая и точка
            comment_without_incidents = re.sub(r'([.,])\s*\1', r'\1', comment_without_incidents)  # Удаляем дубликаты точек и запятых

            comment_without_incidents = re.sub(r'\s+', ' ', comment_without_incidents)  # Заменяем множественные пробелы на один
            comment_without_incidents = re.sub(r'^[\s]*|[\s]*$', '', comment_without_incidents)  # Удаляем пробелы в начале и конце

        return comment_without_incidents.strip()

    def remove_abbreviations(self):
        """Расширяет сокращения в комментарии"""
        abbreviations = {
            r'\bул\.\s*': 'улица ',
            r'\bп\.\s*': 'переулок ',
            r'\bпр\.\s*': 'проспект ',
            r'\bпер\.\s*': 'переулок ',
            r'\bкорп\.\s*': 'корпус ',
            r'\bс\.\s*': 'село ',
            r'\bк\.\s*': 'квартал ',
            r'\bверх\.\s*': 'верхняя ',
            r'\bнижн\.\s*': 'нижняя ',
            r'\bзадвиж\.\s*': 'задвижки ',
            r'\bпониж\.\s*': 'пониженого ',
            r'\bдавл\.\s*': 'давления ',
            r'\bч/д\s*': 'частный дом ',
            r'\bнечет\.\s*': 'нечетный ',
            r'\bчет\.\s*': 'четный ',
            r'\bпос\.\s*': 'поселок ',
            r'\bд\.\s*': 'деревня ',
            r'\b(чет\.)\s*': '',
            r'\b(нечет\.)\s*': '',
            r'\b-я\s*': '',
            r'\b(нечетн\.)\s*': '',
            r'\b(четн\.)\s*': '',
            r'\b(неч\.)\s*': '',
            r'\b([0-9]+эт\.)\s*': '',
        }
        for abbr, full in abbreviations.items():
            self.comment_without_incidents = re.sub(abbr, full, self.comment_without_incidents, flags=re.IGNORECASE)

    def clean_remaining_words(self, text: str) -> str:
        """Удаляет лишние символы и стоп-слова из оставшихся слов."""
        # Удаляем пробелы перед запятой и точкой, оставляя их
        text = re.sub(r'\s+([.,])', r'\1', text)  # Удаляем пробелы перед запятыми и точками

        # Удаляем двойные точки, запятые, точки с запятой и запятые с точками
        text = re.sub(r'\.{2,}', '.', text)  # Двойные точки
        text = re.sub(r',{2,}', ',', text)  # Двойные запятые
        text = re.sub(r';{2,}', ';', text)  # Двойные точки с запятой
        text = re.sub(r'(\.)(,)', r'\1', text)  # Точка и запятая
        text = re.sub(r'(,)(\.)', r'\1', text)  # Запятая и точка
        text = re.sub(r'([.,])\s*\1', r'\1', text)  # Удаляем дубликаты точек и запятых

        text = re.sub(r'\s+', ' ', text)  # Заменяем множественные пробелы на один
        text = re.sub(r'^[\s]*|[\s]*$', '', text)  # Удаляем пробелы в начале и конце

        # Проверяем, пуст ли текст или состоит ли он только из знаков препинания
        if re.fullmatch(r'^[\s.,-]*$', text):
            return ""  # Возвращаем пустую строку, если в тексте нет полезных слов

        if not re.search(r'\w', text):  # Если нет слов (букв или цифр)
            return ''

        return text

    def extract_addresses(self) -> list[str]:
        """Извлекает адреса из комментария и приводит к нужному формату."""
        address_pattern = r'([а-яА-ЯёЁ\s]+).?\,?\s*(\d+[а-яА-ЯёЁ]?((,\d+[а-яА-ЯёЁ]?)|(;|\s|$))*)'
        matches = re.findall(address_pattern, self.comment_without_incidents)

        # Словарь для хранения улиц и их домов
        addresses = {}
        current_street = ""

        for match in matches:
            street = match[0].strip()
            house_numbers = match[1]

            # Обновляем улицу, если найдено её новое название
            if street:
                current_street = street

            # Добавляем дома, разбивая строку по запятым и пробелам
            for number in re.split(r'[,\s]+', house_numbers):
                if number and current_street:  # проверяем, что номер дома и улица не пустые
                    if current_street not in addresses:
                        addresses[current_street] = []
                    if number not in addresses[current_street]:
                        addresses[current_street].append(number)

        # Находим оставшиеся слова после удаления инцидентов и адресов
        remaining_words = re.sub(address_pattern, '', self.comment_without_incidents).strip()
        remaining_words = self.clean_remaining_words(remaining_words)  # Очищаем оставшиеся слова
        self.remaining_words = remaining_words

        # Формируем список адресов с добавлением оставшихся слов
        full_addresses = []
        if not addresses:
            if remaining_words:
                full_addresses = [remaining_words]
        else:
            for street, numbers in addresses.items():
                for number in sorted(numbers, key=lambda x: (int(re.sub(r'\D', '', x)) if re.sub(r'\D', '', x) else float('inf'), x)):
                    address_with_words = f"{street} {number}"
                    if remaining_words:
                        address_with_words += f", {remaining_words}"
                    full_addresses.append(self.clean_remaining_words(address_with_words))

        return full_addresses

    def find_closest_addresses(self, similarity_threshold=30):
        found_addresses = []
        found_uuids = []

        for address in self.extract_addresses:
            best_match = process.extractOne(
                address,
                self.address_to_uuid.keys(),
                scorer=fuzz.ratio
            )
            if best_match and best_match[1] >= similarity_threshold:
                matched_address = best_match[0]
                matched_uuid = self.address_to_uuid[matched_address]
                found_addresses.append(matched_address)
                found_uuids.append(matched_uuid)

        # Если адреса не найдены, добавляем remaining_words как адрес
        if not found_addresses and self.remaining_words:
            cleaned_remaining = self.clean_remaining_words(self.remaining_words)
            if cleaned_remaining:
                found_addresses.append(cleaned_remaining)
                found_uuids.append(None)  # UUID не найден

        return found_addresses, found_uuids

    def write_to_file(self):
        """Записывает результат в указанный CSV файл."""
        with open(results_file, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';')

            for address, uuid in zip(self.found_addresses, self.found_uuids):
                writer.writerow([self.shutdown_id, self.comment, address, uuid])

    def print(self):
        print(self.shutdown_id, " из 3544")
        # print("Shutdown ID:", self.shutdown_id)
        # print("Comment:", self.comment)
        # print("Incidents:", self.incidents)
        # print("Comment without Incidents:", self.comment_without_incidents)
        # print("Remaining Words", self.remaining_words)
        # print("Addresses:", self.extract_addresses)
        # print("Real Addresses:", self.found_addresses)
        # print()


# Преобразуем данные
incidents_data_list: list[IncidentsAddressesData] = [
    IncidentsAddressesData(row['shutdown_id'], str.lower(row['comment'])) for idx, row in tasks_df.iterrows()
]

with open(results_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file, delimiter=';')

    for item in incidents_data_list:
        writer.writerow([item.shutdown_id, item.found_uuids])

