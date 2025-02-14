def get_ngrams(text, n):
    """
    Generate word-based n-grams from a given text string.
    
    Args:
        text (str): Input text/sentence
        n (int): Number of words in each n-gram
    
    Returns:
        set: Set of n-grams where each n-gram is a tuple of words
    """
    # Convert to lowercase and split into words
    words = text.lower().split()
    
    # If the sentence has fewer words than n, return empty set
    if len(words) < n:
        return set()
    
    # Generate n-grams using sliding window over words
    ngrams = set()
    for i in range(len(words) - n + 1):
        # Create tuple of n consecutive words
        ngram = tuple(words[i:i+n])
        ngrams.add(ngram)
    
    return ngrams



def calculate_similarity(reference, candidate, n=2):
    """
    Calculate similarity between two sentences using word-based n-grams 
    and Jaccard similarity.
    
    Args:
        str1 (str): First sentence
        str2 (str): Second sentence
        n (int): Number of words in each n-gram (default: 2)
    
    Returns:
        float: Similarity score between 0 and 1
    """
    # Generate word n-grams for both sentences
    ngrams1 = get_ngrams(reference, n)
    ngrams2 = get_ngrams(candidate, n)
    
    # Handle cases where one or both sentences are too short
    if not ngrams1 and not ngrams2:
        return 1.0 if reference.lower() == candidate.lower() else 0.0
    if not ngrams1 or not ngrams2:
        return 0.0
    
    # Calculate Jaccard similarity
    intersection = len(ngrams1.intersection(ngrams2))
    union = len(ngrams1.union(ngrams2))
    
    return intersection / union



### Remove this lines below if you are putting this in a Lambda function 
## SAMPLE EXPLANATION AND USEAGE

## The two queries below are the same but just written differently ( they return the same result in athena)
# n-gram break a string to small chunks like [(SELECT, s.store) , (s.store, COUNT(st.staff_id),...) for 2-gram 
## it then just counts how many n-grams match between the strings

if __name__ =='__main__':
    reference="SELECT s.store_name, COUNT(st.staff_id) AS num_employees  \
         FROM stores s JOIN staffs st ON s.store_id = st.store_id GROUP BY s.store_name  \
            ORDER BY num_employees DESC LIMIT 20"
    

    candidate= "SELECT store_name, employee_count \
    FROM (\
    SELECT s.store_name, COUNT(st.staff_id) AS employee_count \
    FROM stores s \
    JOIN staffs st ON s.store_id = st.store_id \
    GROUP BY s.store_name, s.store_id \
        ) employee_counts \
        ORDER BY employee_count DESCc\
        LIMIT 20"

    print(calculate_similarity(reference, candidate, n=3))

