import matplotlib.pyplot as plt
import pandas as pd

IN_FILE = "statistics.csv"
DEPTH = "depth"
LOCAL_LEAF_TO_PARENT_RATIO = "(x'+y')/z"
LOCAL_LEAF_RATIO = "x'/y'"

COLOR_PALETTE = [
        "#ff0000","#ff6600","#ffcc00","#ccff00","#66ff00",
        "#00ff00","#00ff66","#00ffcc","#00ccff","#0066ff",
        "#0000ff","#6600ff","#cc00ff","#ff00cc","#ff0066",
        "#ff0033","#ff3300","#ff9900","#ffff00","#99ff00",
        "#33ff00","#00ff33","#00ff99"
]

def main():
    df = pd.read_csv(IN_FILE)
    df = df.dropna()
    x_axis = df[LOCAL_LEAF_TO_PARENT_RATIO].values
    y_axis = df[LOCAL_LEAF_RATIO].values
    colors = [COLOR_PALETTE[v - 1] for v in df[DEPTH].values]

    assert(len(x_axis) == len(y_axis) and len(y_axis) == len(colors))
    
    plt.scatter(x_axis, y_axis, color=colors)
    plt.xlabel(LOCAL_LEAF_TO_PARENT_RATIO)
    plt.ylabel(LOCAL_LEAF_RATIO)
    plt.show()

if __name__ == '__main__':
    main()
