import requests
from bs4 import BeautifulSoup
import time
import random
import csv
import re
from datetime import date, timedelta
import pandas as pd


# Monthly dates generated for iterate
def generate_monthly_dates(start_year, start_month, end_year, end_month):
    current_date = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 1)
    dates = []

    while current_date <= end_date:
        last_day = (current_date + timedelta(days=32)).replace(day=1) - timedelta(
            days=1
        )
        start_str = current_date.strftime("%Y%%2F%m%%2F%d")
        end_str = last_day.strftime("%Y%%2F%m%%2F%d")
        dates.append((start_str, end_str, current_date.year, current_date.month))
        current_date = (current_date + timedelta(days=32)).replace(day=1)
    return dates


# Regex to extract virus names
def extract_virus_names(title):
    virus_match = re.search(r"(\w+[-]*\s*\w+\s*)?(\w+)\s*virus", title, re.IGNORECASE)
    if virus_match:
        virus_name = virus_match.group(1) if virus_match.group(1) else ""
        virus_name += virus_match.group(2) + " virus"
        return virus_name
    return None


# Crawl pubmed from each class
def extract_data(content):
    pmid = content.findAll("span", {"class": "docsum-pmid"})
    title = content.findAll("a", {"class": "docsum-title"})
    author = content.findAll("span", {"class": "docsum-authors short-authors"})
    citation = content.findAll(
        "span", {"class": "docsum-journal-citation full-journal-citation"}
    )
    publication_year = content.findAll(
        "span", {"class": "docsum-journal-citation short-journal-citation"}
    )

    data = []

    for i in range(len(pmid)):
        virus_name = extract_virus_names(title[i].text.strip())
        data.append(
            {
                "PMID": pmid[i].text.strip(),
                "Title": title[i].text.strip(),
                "Author": author[i].text.strip(),
                "Citation": citation[i].text.strip(),
                "Publication Year": publication_year[i].text.strip(),
                "Virus Name": virus_name if virus_name else "",
            }
        )

    return data


# Add all data in the month to csv file
def append_data_to_csv(data_list, filename):
    with open(filename, "a", newline="", encoding="utf-8") as file:
        fieldnames = [
            "PMID",
            "Title",
            "Author",
            "Citation",
            "Publication Year",
            "Virus Name",
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if file.tell() == 0:
            writer.writeheader()

        writer.writerows(data_list)


# Merge all files
def merge_csv_files(filenames, output_filename):
    all_rows = []

    for filename in filenames:
        with open(filename, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            all_rows.extend(list(reader))

    with open(output_filename, "w", newline="", encoding="utf-8") as file:
        fieldnames = [
            "PMID",
            "Title",
            "Author",
            "Citation",
            "Publication Year",
            "Virus Name",
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)


# Start collecting
if __name__ == "__main__":
    print("Starting data collection...")

    monthly_dates = generate_monthly_dates(2020, 1, 2023, 9)
    base_url_other_pages = "https://pubmed.ncbi.nlm.nih.gov/?term=VIRUS&filter=dates.{}-{}&filter=other.excludepreprints&timeline=expanded&size=200&page={}"

    all_monthly_files = []

    for start_str, end_str, year, month in monthly_dates:
        monthly_filename = f"pubmed_data_{year}_{month:02}.csv"
        all_monthly_files.append(monthly_filename)

        base_url = (
            f"https://pubmed.ncbi.nlm.nih.gov/?term=VIRUS&filter=dates.{start_str}-"
            f"{end_str}&filter=other.excludepreprints&timeline=expanded&size=200"
        )

        # Crawl data from the first page
        result = requests.get(base_url)
        bs_obj = BeautifulSoup(result.content, "html.parser")
        content = bs_obj.find("div", {"class": "search-results-chunk results-chunk"})

        if content:
            append_data_to_csv(extract_data(content), monthly_filename)

        # Iterating through the other pages
        # Loop to click the "Load More" button and extract data from multiple pages (starting from page 2)
        for page_number in range(2, 60):
            url = base_url_other_pages.format(start_str, end_str, page_number)

            # Send a GET request to the page
            result = requests.get(url)
            bs_obj = BeautifulSoup(result.content, "html.parser")

            # Extract and append data from the page
            content = bs_obj.find(
                "div", {"class": "search-results-chunk results-chunk"}
            )
            if content:
                append_data_to_csv(extract_data(content), monthly_filename)
            else:
                print(f"Unable to extract data from page {page_number}. Skipping...")

            # Add a random delay before the next request
            delay = random.uniform(1.5, 2.5)
            print(f"Waiting for {delay:.2f} seconds before the next request...")
            time.sleep(delay)

    print("Individual monthly data extracted.")

    # Merging all the files
    all_data = []
    for filename in all_monthly_files:
        df = pd.read_csv(filename)
        all_data.append(df)

    merged_df = pd.concat(all_data, axis=0, ignore_index=True)
    merged_df.to_csv("merged_pubmed_data.csv", index=False)

    print("All data merged into merged_pubmed_data.csv")
