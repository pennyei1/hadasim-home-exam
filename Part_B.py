# Part B: creates tables
#
"""
    Given the following table:
       Person_id | Person_Name | Family_Name | Gender | Father_id | Mother_id | Spouse_id

    Connection_Type = Father, Mother, Brother, Sister, Son, Daughter, Male Partner, Female Partner

    Task 1: Create a family tree
    Task 2: Complete spouses, children, and sibling info

    Note: The input format is assumed to be a .txt file using '|' as a delimiter.
"""
import sys
import csv
from enum import Enum


class ConnectionType(Enum):
    """
        Enum for types of relationships within a family tree.
    """
    FATHER = "Father"
    MOTHER = "Mother"
    BROTHER = "Brother"
    SISTER = "Sister"
    SON = "Son"
    DAUGHTER = "Daughter"
    MALE_PARTNER = "Male partner"
    FEMALE_PARTNER = "Female partner"


def parse_table(file):
    """
        Parses the table from the given file and returns a list of dictionaries,
        where each dictionary represents a person with cleaned key-value pairs.

        :param file: Path to the input text file
        :return: List of dictionaries containing person records
    """
    persons = []
    with open(file, 'r', newline='') as inFile:
        reader = csv.DictReader(inFile, delimiter='|')
        for row in reader:
            # Trim whitespace from keys and values, convert empty strings to None
            clean_row = {}
            for k, v in row.items():
                key = k.strip()
                if v and v.strip():
                    value = v.strip()
                else:
                    value = None
                clean_row[key] = value
            persons.append(clean_row)
    return persons


def create_family_tree(persons_dict):
    """
        Constructs the family tree by identifying parents, spouses, siblings, and children
        from the given list of people records.

        The resulting relationship data is saved into a file named 'family_tree.txt'.

        :param persons_dict: List of dictionaries representing people and their relationships
    """
    family_tree = []

    # 2. before making the family tree, fill the missing spouses info in case of missing info
    complete_spouses(persons_dict)

    for person in persons_dict:
        pid = person['Person_Id']
        gender = person['Gender']
        father_id = person['Father_Id']
        mother_id = person['Mother_Id']
        spouse_id = person['Spouse_Id']

        # Add father relationship
        if father_id is not None:
            family_tree.append({
                "Person_Id": pid,
                "Relative_Id": father_id,
                "Connection_Type": ConnectionType.FATHER
            })

        # Add mother relationship
        if mother_id is not None:
            family_tree.append({
                "Person_Id": pid,
                "Relative_Id": mother_id,
                "Connection_Type": ConnectionType.MOTHER
            })

        # Add spouse relationship
        if spouse_id is not None:
            type_spouse = ConnectionType.FEMALE_PARTNER if gender == "M" else ConnectionType.MALE_PARTNER
            family_tree.append({
                "Person_Id": pid,
                "Relative_Id": spouse_id,
                "Connection_Type": type_spouse
            })

    # Add sibling and children relationships
    find_siblings(family_tree, persons_dict)
    find_kids(family_tree, persons_dict)

    # Sort by Person_Id then Relative_Id
    family_tree_sorted = sorted(family_tree,
                                key=lambda row: (int(row["Person_Id"]), int(row['Relative_Id'])))

    # Write to output file
    with open("family_tree.txt", 'w') as file:
        file.write("Person_Id | Relative_Id | Connection_Type\n")
        for row in family_tree_sorted:
            record = f"{row['Person_Id']:>9} | {row['Relative_Id']:>11} | {row['Connection_Type'].value}\n"
            file.write(record)


def find_siblings(family_tree, persons_dict):
    """
        Adds sibling relationships to the family tree.
        Two people are siblings if they share the same non-null mother and father.

        :param family_tree: List to append new relationship records to
        :param persons_dict: List of people records
    """
    for person in persons_dict:
        pid = person['Person_Id']
        father_id = person['Father_Id']
        mother_id = person['Mother_Id']

        for other in persons_dict:
            oid = other['Person_Id']
            other_father_id = other['Father_Id']
            other_mother_id = other['Mother_Id']
            gender = other['Gender']

            if (pid != oid and father_id and mother_id and
                    father_id == other_father_id and mother_id == other_mother_id):
                type_relation = ConnectionType.BROTHER if gender == "M" else ConnectionType.SISTER
                family_tree.append({
                    "Person_Id": pid,
                    "Relative_Id": oid,
                    "Connection_Type": type_relation
                })


def find_kids(family_tree, persons_dict):
    """
        Adds child relationships to the family tree.
        A person is a parent of another if their ID appears in the child's father_id or mother_id.

        :param family_tree: List to append new relationship records to
        :param persons_dict: List of people records
    """
    for parent in persons_dict:
        pid = parent['Person_Id']
        for child in persons_dict:
            cid = child['Person_Id']
            father_id = child['Father_Id']
            mother_id = child['Mother_Id']
            gender = child['Gender']

            if father_id == pid or mother_id == pid:
                type_relation = ConnectionType.SON if gender == "M" else ConnectionType.DAUGHTER
                family_tree.append({
                    "Person_Id": pid,
                    "Relative_Id": cid,
                    "Connection_Type": type_relation
                })


def complete_spouses(persons_records):
    # dict such that key-value is: (Person_ID, Person_dict)
    id_dict = {person['Person_Id']: person for person in persons_records}
    for person in persons_records:

        spouse_id = person.get('Spouse_Id')
        if spouse_id:
            spouse = id_dict.get(spouse_id)
            if spouse and spouse.get('Spouse_Id') != person['Person_Id']:
                spouse['Spouse_Id'] = person['Person_Id']


if __name__ == '__main__':
    """
        Entry point: Parses the input table and creates the family tree file.
    """
    # enter a table file in the format .txt
    file = sys.argv[1]
    persons_table = parse_table(file)
    create_family_tree(persons_table)
    print("finish creating tables")
