import re


def get_formated_telephone(telephone):
    if not telephone:
        return ""

    telephone = re.sub('[^0-9/]', '', telephone)

    if telephone.startswith('0'):
        telephone = telephone[1:]

    if telephone.startswith("549"):
        telephone = telephone[3:]

    telephones = telephone.split("/")

    return telephones[0]
