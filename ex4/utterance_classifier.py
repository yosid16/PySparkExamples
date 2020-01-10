from random import random
from random import randint

"""
Classifies if utterance is offensive or not.
"""
class UtteranceClassifier:
    def __init__(self):
        self.error_percentage = randint(0, 10) / 100.0

    def classify(self, utterance):
        print("classifying: {}".format(utterance))
        should_throw_exception = random() < self.error_percentage
        if should_throw_exception:
            raise Exception("Could not classify given utterance")
        return self._is_offensive(utterance)

    def _is_offensive(self, utterance):
        return True if random() < 0.1 else False