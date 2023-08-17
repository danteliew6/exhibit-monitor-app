from xml.dom.minidom import parse, parseString

def main():
    doc = parse("config.xml")
    file_structure = doc.getElementsByTagName("fileStructure")
    print(file_structure)

if __name__ == "__main__":
    main()