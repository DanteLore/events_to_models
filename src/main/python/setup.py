from ksql import KSQLAPI

# https://pypi.org/project/ksql/

if __name__ == "__main__":
    client = KSQLAPI('http://localhost:8088')

    print("Setting Up!")

    lines = []
    with open("../ksql/beer-fest.ksql") as f:
        lines += [l.strip() for l in f.readlines()]

    lines = [l for l in lines if l]

    for line in lines:
        if line[0] == '#':
            print(line)
            raw_input("Press a key...")
        elif line[0] == '$':
            print(line)
            raw_input("Execute the above command please!")
        else:
            print(line)
            result = client.ksql(line)
            print(result)


