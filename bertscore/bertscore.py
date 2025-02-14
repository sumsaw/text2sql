from bert_score import BERTScorer

def bertscorer(reference,candidate):
   
    # BERTScore calculation
    scorer = BERTScorer(model_type='bert-base-uncased')
    P, R, F1 = scorer.score([candidate], [reference])
    print(f"BERTScore Precision: {P.mean():.4f}, Recall: {R.mean():.4f}, F1: {F1.mean():.4f}")


if __name__=='__main__':

     # Example texts
    reference = "SELECT s.store_name, COUNT(st.staff_id) AS num_employees  \
            FROM stores s JOIN staffs st ON s.store_id = st.store_id GROUP BY s.store_name  \
                ORDER BY num_employees DESC LIMIT 20"


    candidate = "SELECT store_name, employee_count \
        FROM (\
        SELECT s.store_name, COUNT(st.staff_id) AS employee_count \
        FROM stores s \
        JOIN staffs st ON s.store_id = st.store_id \
        GROUP BY s.store_name, s.store_id \
            ) employee_counts \
            ORDER BY employee_count DESCc\
            LIMIT 20"
    
    bertscorer(reference,candidate)