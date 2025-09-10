import re

def convert_and_parse(txt_path):
    with open(txt_path, "r", encoding="utf-8") as f:
        text = f.read()

    # 1️⃣ Name (first line with two capitalized words)
    name_match = re.search(r"([A-Z][a-z]+(?:\s[A-Z][a-z]+)+)", text)
    name = name_match.group(1) if name_match else None

    # 2️⃣ Email
    email_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    email = email_match.group(0) if email_match else None

    # 3️⃣ Contact Number
    phone_match = re.search(r"(\+?\d{1,3}[-.\s]?)?(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", text)
    contact_number = phone_match.group(0) if phone_match else None

    # 4️⃣ Summary
    summary_match = re.search(r"(Summary|Profile|Objective)\s*[:\-]?\s*(.*?)(?=\n[A-Z])", text, re.IGNORECASE | re.DOTALL)
    summary = summary_match.group(2).strip() if summary_match else None

    # 5️⃣ Skills
    skills_match = re.search(r"(Skills|Technical Skills|Core Competencies)\s*[:\-]?\s*(.*?)(?=\n[A-Z])", text, re.IGNORECASE | re.DOTALL)
    skills = skills_match.group(2).strip() if skills_match else None

    # 6️⃣ Education
    education_match = re.search(r"(Education|Academic Background)\s*[:\-]?\s*(.*?)(?=\n[A-Z])", text, re.IGNORECASE | re.DOTALL)
    education = education_match.group(2).strip() if education_match else None

    # 7️⃣ Professional Experience
    experience_match = re.search(r"(Professional Experience|Work Experience|Employment History)\s*[:\-]?\s*(.*?)(?=\n[A-Z])", text, re.IGNORECASE | re.DOTALL)
    experience = experience_match.group(2).strip() if experience_match else None

    return {
        "Name": name,
        "Email": email,
        "ContactNumber": contact_number,
        "Summary": summary,
        "Skills": skills,
        "Education": education,
        "ProfessionalExperience": experience,
        "TxtPath": txt_path
    }
