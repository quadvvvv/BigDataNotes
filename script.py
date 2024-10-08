
idx = 1
output_base = "SparkNotesSection-"
curr = None

print("Proceeding")

with open("notes.md") as source:
    line = source.readline()
    while line:
        if(line.startswith("## ")):
            print("Writing to new file ")
            curr = open(output_base+str(idx)+".md", "w")
            idx+=1
        curr.write(line)
        line = source.readline()



