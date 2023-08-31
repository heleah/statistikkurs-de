import tkinter as tk
from full_dl_master import *
from connection_test import *

def full_dl():
    run_full_dl_master()

def update():
    print("hier wird in Zukunft das Update ausgeführt") 

# Create main window
root = tk.Tk()
root.title("Julian's Tool")

label = tk.Label(root,text="Was möchtest du tun?")
label.pack()

# Create buttons
full_dl_button = tk.Button(root, text="Full Download", command=full_dl)
full_dl_button.pack()

update_button = tk.Button(root, text="Update", command=update)
update_button.pack()

connection_button = tk.Button(root, text="Test Connection", command=run_connection_test)
connection_button.pack()

# Start the GUI event loop
root.mainloop()
