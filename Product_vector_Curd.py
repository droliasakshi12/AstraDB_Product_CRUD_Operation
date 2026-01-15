from astrapy import DataAPIClient
import pandas as pd
import streamlit as st


@st.cache_resource
def init_db():
    client = DataAPIClient(
        token="AstraCS:rWSHEwBRNcRTONNXOzUoifXZ:2395eeccde459698c09dd06663a4f831535b465c730bf134414ee17ce195f773")
    db = client.get_database_by_api_endpoint(
        "https://fc8bc821-93ad-4b39-981c-1d2076930056-us-east1.apps.astra.datastax.com")
    return db


db = init_db()
collection_name = "product_catalog"
vector_dimensions = 5


def get_collections():
    existing_collection = db.list_collection_names()
    try:
        if collection_name not in existing_collection:
            collection = db.create_collection(
                name=collection_name,
                definition={
                    "vector": {
                        "dimension": vector_dimensions,
                        "metric": "cosine"
                    }
                }
            )
            st.success(f"collection {collection_name} created successfully")
        else:
            collection = db.get_collection(collection_name)
            return collection

    except Exception as e:
        st.error(f"ERROR{e}")
        return None


def vector_from_string(vector_input):
    try:
        return [float(x.strip()) for x in vector_input.split(",")]
    except:
        st.error("invalid  vector format.use comma seprated values ")
        return None


# calling collection function to check if collection collection exists
collection = get_collections()

st.title("ASTARDB VECTOR DATABASE ")
st.markdown("--------------------")
st.sidebar.title("NAVIGATION!!")
page = st.sidebar.radio("SELECT OPERATION", [
                        "view_data", "add_data", "update_data", "delete_data"])

# viewing prodcut catalog
if collection:
    if page == "view_data":
        st.title("VIEW PRODUCTS")
        view_by = st.radio(
            "CHOOSE VIEW", ["ALL PRODUCTS", "VIEW BY ID", "VIEW BY CATEGORY"])
        # view all products #by id #product by category
        try:
            if view_by == "ALL PRODUCTS":
                view_product = list(collection.find({}))
                view_list = []
                for view in view_product:
                    data = view_list.append({
                        "ID": view.get("_id"),
                        "NAME": view.get("name"),
                        "CATEGORY": view.get("category"),
                        "PRICE": view.get("price"),
                        "DESCRIPTION": view.get("description"),
                        "VECTOR": view.get("$vector")
                    })
                df = pd.DataFrame(view_list)
                st.dataframe(df, use_container_width=True)
                st.success(f"TOTAL PRODUCTS ARE {len(view_list)}")

        except Exception as e:
            st.error(e)

        # viewing by id
        try:
            if view_by == "VIEW BY ID":
                product_id = st.text_input(
                    label="PRODUCT ID", placeholder="Ex:prod_001")
                find_id = st.button("FIND PRODUCT")
                if find_id:
                    view_by_id = collection.find_one({"_id": product_id})
                    if view_by_id:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**ID**:", view_by_id["_id"])
                            st.write("**NAME**:", view_by_id["name"])
                            st.write("**CATEGORY**:", view_by_id["category"])

                        with col2:
                            st.write("**PRICE**:", view_by_id["price"])
                            st.write("**DESCRIPTION**:",
                                     view_by_id["description"])
                            # st.write("**VECTOR**:",view_by_id["$vector"])
        except Exception as e:
            st.error(e)

        # viewing by category
        try:
            if view_by == "VIEW BY CATEGORY":
                find_cat = list(collection.find({}))
                # made set so that  the values does not repeat
                category = set([i["category"] for i in find_cat])
                select_category = st.selectbox("SELECT CATEGORY", category)
                select_product = collection.find({"category": select_category})
                view_product = []
                for p in select_product:
                    view_by_cat = view_product.append({
                        "ID": p.get("_id"),
                        "NAME": p.get("name"),
                        "CATEGORY": p.get("category"),
                        "PRICE": p.get("price"),
                        "DESCRIPTION": p.get("description")
                    })
                df = pd.DataFrame(view_product)
                st.dataframe(df)
                st.success(f"TOTAL PRODUCT {len(view_product)}")

        except Exception as e:
            st.error(e)

# adding new products
    elif page == "add_data":
        st.header("ADD NEW PRODUCT")

        with st.form("add new product"):
            id = st.text_input(label="PRODUCT ID ", placeholder="prod_001")
            name = st.text_input(label="PRODUCT NAME", placeholder="LAPTOP")
            categories = st.text_input(
                label="PRODUCT CATEGORY", placeholder="Electronics")
            price = st.number_input(label="PRICE")
            description = st.text_area(label="DESCRIPTION")
            vector_input = st.text_input(
                label="VECTOR", placeholder="0.1,0.2,0.3,0.4,0.5")

            submit_product = st.form_submit_button("ADD PRODUCT")

            if submit_product:
                # basic validation
                if not all([id, name, categories, description, vector_input]):
                    st.warning("ALL FIELDS ARE REQUIRED")
                else:
                    try:
                        vector = vector_from_string(vector_input)

                        if vector and len(vector) == vector_dimensions:
                            add_product = {
                                "_id": id,
                                "name": name,
                                "category": categories,
                                "price": price,
                                "description": description,
                                "$vector": vector,
                            }

                            # use insert_one for a single product
                            collection.insert_one(add_product)
                            st.success("NEW PRODUCT ADDED")

                        else:
                            st.error(
                                f"Vector size must be {vector_dimensions} dimensions")

                    except Exception as e:
                        st.error(f"{e}")

# update product
    if page == "update_data":
        st.header("UPDATE PRODUCT")

        prod_id = st.text_input("PRODUCT ID", placeholder="Enter the ID of the product to update")
        find_product = st.button("FIND PRODUCT")

        if find_product:
            fetch_prod_data = collection.find_one({"_id":prod_id})

            if fetch_prod_data:
                st.success("✅ PRODUCT FOUND")

                with st.form("update_product_form"):
                    update_prod_name = st.text_input(
                        "PRODUCT NAME", value=fetch_prod_data.get("name", ""))
                    update_category = st.text_input(
                        "PRODUCT CATEGORY", value=fetch_prod_data.get("category", ""))
                    update_price = st.number_input(
                        "PRODUCT PRICE", value=float(fetch_prod_data.get("price", 0.0)))
                    update_description = st.text_area(
                        "PRODUCT DESCRIPTION", value=fetch_prod_data.get("description", ""))
                    update_vector_input = st.text_input(
                        "VECTOR", value=",".join(map(str, fetch_prod_data.get("$vector", []))))

                    update_submit = st.form_submit_button("UPDATE PRODUCT")

                    if update_submit:
                        try:
                            new_vector = vector_from_string(update_vector_input)
                            if new_vector and len(new_vector
                                                  ) == vector_dimensions:
                                collection.update_one(
                                    {"_id": prod_id},
                                    {"$set": {
                                        "name": update_prod_name,
                                        "category": update_category,
                                        "price": update_price,
                                        "description": update_description,
                                        "$vector": new_vector
                                    }}
                                )
                                st.success("✅ PRODUCT Updated Successfully!")
                            else:
                                st.error("Vector length mismatch or invalid vector input.")
                        except Exception as e:
                            st.error(f"Error updating product: {e}")

            else:
                st.error("❌ No product found with that ID.")
