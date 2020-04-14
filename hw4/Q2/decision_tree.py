import numpy as np 
import ast
from util import entropy, information_gain, partition_classes, best_split

class DecisionTree(object):
    def __init__(self, max_depth):
        # Initializing the tree as an empty dictionary or list, as preferred
        self.tree = {}
        self.max_depth = max_depth

    def learn(self, X, y, par_node = {}, depth=0):
        # TODO: Train the decision tree (self.tree) using the the sample X and labels y
        # You will have to make use of the functions in utils.py to train the tree

        # Use the function best_split in util.py to get the best split and 
        # data corresponding to left and right child nodes
        
        # One possible way of implementing the tree:
        #    Each node in self.tree could be in the form of a dictionary:
        #       https://docs.python.org/2/library/stdtypes.html#mapping-types-dict
        #    For example, a non-leaf node with two children can have a 'left' key and  a 
        #    'right' key. You can add more keys which might help in classification
        #    (eg. split attribute and split value)
        ### Implement your code here
        #############################################
        y1 = np.array(y)
        self.tree_depth = 0
        # if has only one class, or too many one class
        y_counts = np.bincount(y1.astype(int))
        #print(y_counts)
        if self.max_depth < self.tree_depth:
            #print("yes1")
            self.tree['status'] = 'leaf'
            self.tree['value'] = np.argmax(y_counts)
            return

        if len(y_counts) == 1:
            #print("yes2")
            self.tree['status'] = 'leaf'
            self.tree['value'] = 0
            return

        if y_counts[0] == 0:
            self.tree['status'] = 'leaf'
            self.tree['value'] = 1
            return
        #just use this to speed up, you can delete it for higher accuracy
        '''
        if (y_counts[1]/y_counts[0] > 0.95) or (y_counts[1]/y_counts[0] < 0.05):
            self.tree['status'] = 'leaf'
            self.tree['value'] = np.argmax(y_counts)
            return
        '''
        #print("yes")
        best_en, split_attribute, split_val, X_left, X_right, y_left, y_right = best_split(X,y)
        #print(best_en)
        if best_en == 0 or best_en == 1: 
            self.tree['status'] = 'leaf'
            self.tree['value'] = np.argmax(y_counts)
            return
        self.tree['status'] = "parent"
        self.tree['attr'] = split_attribute
        self.tree['val'] = split_val
        self.tree['left'] = DecisionTree(self.max_depth)
        self.tree['right'] = DecisionTree(self.max_depth)
        self.tree["left"].learn(X_left, y_left)
        self.tree["right"].learn(X_right, y_right)
        self.tree['left'].tree_depth = self.tree_depth + 1
        self.tree['right'].tree_depth = self.tree_depth + 1
        #############################################


    def classify(self, record):
        # TODO: classify the record using self.tree and return the predicted label
        ### Implement your code here
        #############################################
        if self.tree['status']=='leaf':
            return self.tree['value']
        elif record[self.tree['attr']] <= self.tree['val']:
            return self.tree['left'].classify(record)

        else:
            return self.tree['right'].classify(record)
        #############################################