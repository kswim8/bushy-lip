import matplotlib.pyplot as plt

IN_FILE = "statistics.txt"
NOT_NUMBER = "----"

def read_in_file(in_file, x_axis, y_axis):
    for line in in_file:
        line = line.strip()
        row = line.split()
        
        try:
            children_to_parent_ratio = float(row[-2])
            children_ratio = float(row[-1])

            x_axis.append(children_to_parent_ratio)
            y_axis.append(children_ratio)
        except Exception as e:
            continue

def plot_data(x_axis, y_axis):
    plt.scatter(x_axis, y_axis)
    
    plt.xlabel("[x/y]")
    plt.ylabel("[(x+y)/z]")
    
    plt.show()

def main():
    
    x_axis = []
    y_axis = []
    with open(IN_FILE, 'r') as in_file:
        read_in_file(in_file, x_axis, y_axis)

        assert(len(x_axis) == len(y_axis))

        plot_data(x_axis, y_axis)

if __name__ == '__main__':
    main()

