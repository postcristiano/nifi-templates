import sys, os, json

class AsmPmuHarmonizer():
    from abbs import abbs

    def __init__(self):
        """
            ToDo: Correct id for PMU
        """
        self.__id = "urn:ngsi-ld:"
        self.__context = ["https://smartdatamodels.org/context.jsonld", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-contest.jsonld"]
        self.original_data_dir = ""
        self.harmonized_data_dir = ""


    def __harmonize_data__(self, input_ob):
        """Harmonizes the input JSON object and returns it as JSON-LD string"""
        """
            input_data : file content in str
            topic_name : str


            ToDo: get the correct date 
        """
        out_ob = {}

        out_ob["id"] = self.__id
        out_ob["dateObserved"] = "" # empty for now 
        out_ob["@context"] = self.__context

        for key in input_ob:
            if key in self.abbs:
                out_ob[self.abbs[key]] = input_ob[key]

        return json.dumps(out_ob)

    def __read_and_write_harmonized__(self, topic_name):
        """Reads original data, writes out harmonized"""
        dir_path = self.original_data_dir + os.path.sep + topic_name
        write_path = self.harmonized_data_dir + os.path.sep + topic_name

        if (not os.path.exists(write_path)):
            os.makedirs(write_path)

        for f in os.listdir(dir_path):
            with open(dir_path + os.path.sep + f, "r") as f_data:
                json_ob = json.loads(f_data.read())
                harmonized_data = self.__harmonize_data__(json_ob)
                with open(write_path + os.path.sep + f, "w") as w_file:
                    w_file.write(harmonized_data)
                

    def __read_from_stdin_and_write__(self):
        """Reads the whole file from stding, harmonizes it and sends it to stdout(for NiFi)"""
        
        topic_name = sys.argv[1] # Will be passed via NiFi processor
        input_data = sys.stdin.read()

        harmonzed_data = self.__harmonize_data__(json.loads(input_data))
        sys.stdout.write(harmonzed_data)

    def execute(self, original_data_dir="", stdin=False):
        """Retreives data depending on the situation, either from local files or stdin(for NiFi)"""

        if bool(original_data_dir) == bool(stdin):
            sys.stderr("Wrong usage of the function, only one parameter needed")
            return
        elif original_data_dir:
            self.set_original_data_dir(original_data_dir)
            for topic_dir in os.listdir(self.original_data_dir):
                self.__read_and_write_harmonized__(topic_dir)
        else:
            self.__read_from_stdin_and_write__()


    def set_original_data_dir(self, original_data_dir):
        self.original_data_dir = original_data_dir
        self.harmonized_data_dir = os.path.dirname(original_data_dir) + os.path.sep + "harmonized_data"
        if not os.path.exists(self.harmonized_data_dir):
            os.makedirs(self.harmonized_data_dir)



def main():
    harmonizer = AsmPmuHarmonizer()
    harmonizer.execute(original_data_dir="/path/to/pmu/data")

if __name__ == "__main__":
    main()
